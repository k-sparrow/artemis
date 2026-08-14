# Docling Serve

**Version:** ghcr.io/docling-project/docling-serve-cu128:v1.29.0  
**Role:** Document parsing. Converts PDFs, DOCX, HTML, and other formats into structured
output: page-delimited Markdown and semantic chunks with content type labels (TEXT, TABLE,
SECTION_HEADER, etc.). GPU-accelerated in dev; the Ray engine (below) runs CPU-only in
`test` compose without any loss of correctness, just slower.

**Compose profile:** `ai`  
**Port:** 5001 (internal only, not exposed to host)

**Engine:** Ray-backed in all three compose modes (`DOCLING_SERVE_ENG_KIND=ray`) — server-side
PDF page-slice fan-out across a small Ray cluster (`ray-head` + `ray-worker`, GCS backed by
`redis`, unrelated to Celery's RabbitMQ broker). `release` was promoted ahead of a large-PDF
(400+ page) memory-bounding proof — correctness was verified at 50 pages/5 slices, not at
production scale; an explicit, deliberate risk-acceptance call, not an oversight. See TODOs.md
Epic 21 for the full cluster topology and rollout history.

---

## What It Does

Docling Serve wraps the [Docling](https://github.com/DS4SD/docling) document understanding
library behind an HTTP API. It produces a `DoclingDocument` — a structured representation
of the document with:

- Per-page content in Markdown
- Semantic chunks with labels from `DocItemLabel` (TEXT, TABLE, SECTION_HEADER, CAPTION, etc.)
- Layout and hierarchy metadata

The parsing service uses two Docling Serve endpoint groups:

1. **Conversion** — converts a document to `DoclingDocument` JSON
2. **Chunking** — applies the hybrid chunker to a `DoclingDocument`, returning `ChunkedDocumentResult`

---

## API Usage

### Single document (small files)

```
POST /v1/convert/file/async
Content-Type: multipart/form-data

files=@document.pdf
to_formats=json
```

Returns `{"task_id": "<uuid>"}` immediately. Poll
`GET /v1/status/poll/{task_id}?wait=0` until `task_status` is terminal, then fetch
the result with `GET /v1/result/{task_id}`.

**Only `/v1/convert/source/batch` can take an S3 source, and only with the Ray-serde patch.**
`/v1/convert/source/async`'s `sources` field is typed `FileSourceRequest | HttpSourceRequest`
only — an S3 source is a schema-level `422` there, confirmed directly against
`docling.datamodel.service.requests.ConvertSourcesRequest`. Only `/v1/convert/source/batch`'s
`sources` union includes `S3SourceRequest` — despite the name, still a single-request, async,
`task_id`-pollable call through the same orchestrator, not a real batch operation.

S3-sourced documents submitted there are routed through the Ray orchestrator's
`_is_s3_fanout_task` codepath (`docling_jobkit.orchestrators.ray.serve_deployment`), which used
to fail every document instantly: `SourceChunkConvertRequest.chunk` was typed as the
*subscripted* generic `DocumentChunk[Any, Any]` rather than the bare `DocumentChunk` class.
Pydantic coerces the field's value into a dynamically-created parameterized class with no
stable, importable qualname, which Ray Serve's cross-replica-process pickling of that value
(coordinator → converter replica) can't reconstruct —
`ray.exceptions.RaySystemError: System error: 'type'`, every document failing in under 100ms
with the task status still reporting `"success"` at the top level. Root-caused and fixed with a
one-line change (drop the `[Any, Any]`); see `tools/oci/images/docling/` for the patch, applied
as a layer on top of the vanilla image and used across dev/test/release. Not fixed upstream as
of v1.30.0; no matching issue filed yet.

`/v1/convert/source/batch`'s `target` has no inbody option (`S3Target | AzureBlobTarget |
GoogleCloudStorageTarget | GoogleDriveTarget | PresignedUrlTarget`) — `PresignedUrlTarget`
doesn't work against locally-deployed S3 clusters, so using this endpoint at all means the
result comes back via `S3Target` rather than inline; the parsing service discovers the written
key via a recursive listing scoped to a per-`obj_id` scratch prefix, since docling-jobkit nests
the artifact under path segments not fully under its control.

The parsing service uses this path only for `source_ref` (S3 claim-check) inputs — it verifies
the object exists first (never reads it), then passes the bucket/key straight through. Inline
`file` uploads still go through `/v1/convert/file/async` unchanged.

### Large PDFs — server-side page-slice fan-out (Ray engine)

Every document — regardless of size — goes through the same single-submission call (`file` via
`/v1/convert/file/async`, `source_ref` via `/v1/convert/source/batch` — see above). There is no
separate large-PDF code path on the client side any more; a prior version of this doc described
client-side PDF sharding via `/v1/convert/source/batch`, which was retired once the Ray engine
made it unnecessary — see TODOs.md Epic 21. `/v1/convert/source/batch` is back in use, but for
an unrelated reason (it's the only endpoint whose schema accepts an S3 source), not for
client-side sharding.

With `DOCLING_SERVE_ENG_KIND=ray` (all three compose modes), Docling Serve itself splits large
PDFs into page slices and converts them concurrently across Ray actors on `ray-worker`, entirely
transparent to callers — one `task_id`, one poll loop, one result, same as a small document.
Concurrency and slice size are tuned via `DOCLING_SERVE_ENG_RAY_MAX_PAGE_SLICE_SIZE` and the
`DOCLING_SERVE_ENG_RAY_MAX_CONCURRENT_TASKS` / `MAX_ONGOING_REQUESTS_PER_REPLICA` settings —
see the `docling-serve` service block in `tools/docker/docker-compose.tmpl.yaml` for current
values.

### Hybrid chunking

Chunking is a fully decoupled stage from conversion in the parsing service (Epic 21) —
it happens after `/v1/parse/resolve` has written the converted `DoclingDocument` to its
replay cache, via a separate `/v1/chunk/submit` call. Like convert, no chunk endpoint can
actually take an S3 source either (`BaseChunkDocumentsRequest.sources` has the identical
`FileSourceRequest | HttpSourceRequest`-only restriction, and there's no batch variant of
`/v1/chunk/*/source/async` to even attempt the Ray-fanout workaround with — this docling-serve
version has no chunk batch endpoint at all). So the parsing service reads the replay-cached
`DoclingDocument` itself and submits it inline:

```
POST /v1/chunk/hybrid/file/async
Content-Type: multipart/form-data

files=@doc.json
```

This is the same endpoint non-production callers with no replay cache to read from use (e.g.
`experiments/eval/runners/retrieval_eval.py`) — there's no separate "production" chunk path.

Returns `{"task_id": "<uuid>"}`. Fetch result with `GET /v1/result/{task_id}` →
`{"chunks": [...]}`.

`target` for chunk endpoints always defaults to in-body (`InBodyTarget()`) — unlike convert's
`target`, it isn't policy-configurable via `resolve_default_target`.

---

## ConversionStatus

All task types share the same status model:

| Status | Meaning |
|---|---|
| `pending` | Queued, not yet picked up by a worker |
| `started` | Worker has claimed the task; conversion may or may not have begun |
| `success` | Terminal: conversion completed successfully |
| `failure` | Terminal: conversion failed |
| `partial_success` | Terminal: some page slices failed under the Ray engine's server-side fan-out |
| `skipped` | Terminal: document(s) skipped |

`task_meta.num_processed` / `num_total` population under the Ray engine's page-slice fan-out
isn't a documented contract — the parsing service reads them opportunistically and never
requires them to be set. The local engine does not populate them at all.

Status transitions: `pending` → `started` → `success` | `failure` (or `partial_success` /
`skipped`). `started` is set synchronously when the task is dequeued, before conversion begins.

---

## Table Handling

- **SEMI_STRUCTURED pipeline:** `split_tables=False` — tables kept whole as single chunks
- **SIMPLE pipeline:** `split_tables=True` — tables split at the chunk boundary

---

## CUDA Version

The image `docling-serve-cu128:v1.29.0` requires CUDA 12.8. If your GPU does not support
CUDA 12.8, use a compatible image version — or run without a GPU at all, which now works
regardless of engine (the Ray engine's actual conversion work runs fine CPU-only, just
slower; this is how `test` compose runs it).

---

## Server-side timeout

`DOCLING_SERVE_MAX_SYNC_WAIT` is set to `12` seconds in compose (the default). The
parsing service only ever calls `/async` endpoints — it never calls the synchronous
`/v1/convert/file` endpoint, so the sync timeout is irrelevant for production traffic.

---

## Health Check

The parsing service's readiness probe checks Docling reachability:

```bash
curl http://docling-serve:5001/health
```

If Docling Serve is down, the parsing service returns `503 Service Unavailable` on
`POST /v1/parse/submit` immediately (circuit breaker opens after 3 failures).
