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

### Large PDFs — server-side page-slice fan-out (Ray engine)

Every document — regardless of size — goes through the exact same `/v1/convert/file/async`
call above. There is no separate large-PDF code path on the client side any more (a prior
version of this doc described one: client-side PDF sharding + a `/v1/convert/source/batch`
S3 workflow. That was retired outright once the Ray engine made it unnecessary — see
TODOs.md Epic 21).

With `DOCLING_SERVE_ENG_KIND=ray` (all three compose modes), Docling Serve itself splits large
PDFs into page slices and converts them concurrently across Ray actors on `ray-worker`, entirely
transparent to callers — one `task_id`, one poll loop, one result, same as a small document.
Concurrency and slice size are tuned via `DOCLING_SERVE_ENG_RAY_MAX_PAGE_SLICE_SIZE` and the
`DOCLING_SERVE_ENG_RAY_MAX_CONCURRENT_TASKS` / `MAX_ONGOING_REQUESTS_PER_REPLICA` settings —
see the `docling-serve` service block in `tools/docker/docker-compose.tmpl.yaml` for current
values.

### Result delivery: `S3Target` vs `inbody` vs `PresignedUrlTarget`

`docling.datamodel.service.targets.Target` is a discriminated union with several kinds. Three
matter to Artemis:

| Target kind | Behavior | Usable for chunk endpoints? |
|---|---|---|
| `InBodyTarget` (`inbody`) | Result returned inline in the `GET /v1/result/{task_id}` response body | Yes (default) |
| `S3Target` | An `S3Coordinates` — docling-serve writes the result directly into the given bucket/`key_prefix`, `GET /v1/result/{task_id}` then returns an empty `RemoteTargetResult` | Yes |
| `PresignedUrlTarget` | docling-serve computes a presigned URL itself and returns it in the result body (`ArtifactRef.uri`) | **No** — `app.py` returns `422` with `"presigned_url target is not supported for chunk endpoints"` for both `/v1/chunk/*/file/async` and `/v1/chunk/*/source/async` |

Production submissions (`submit_source`, `submit_chunk_source`) use `S3Target` for **both**
convert and chunk *results*, pointing `key_prefix` at a scratch prefix in the parsing service's
own `REPLAY_CACHE_BUCKET` (Epic 21 §21.9 — avoids pulling a potentially large `DoclingDocument`
JSON, or a large chunk list, through an HTTP response body). `PresignedUrlTarget` — despite
being `ConvertSourcesRequest`'s own default — was considered and rejected for the convert side
too, for consistency (using one target kind everywhere rather than mixing) and because it
doesn't work against locally-deployed S3 clusters (confirmed directly in code in a prior
session, not just documentation). `InBodyTarget` remains in use for `submit_file`/
`submit_chunk` (multipart, inline-bytes paths) — those are non-production callers
(`experiments/eval/runners/retrieval_eval.py`, `test_ray_engine.py`) that never had a large-payload
problem to begin with.

**The *source* side is more restricted than the target side, and differs between convert and
chunk** — this was gotten wrong in an earlier pass of Epic 21 §21.9 and hit as a live `422` on
`/v1/convert/source/async` before being corrected. Confirmed directly against docling-serve
v1.29.0's request models:

- `ConvertSourcesRequest.sources` (used by `/v1/convert/source/async`) and
  `BaseChunkDocumentsRequest.sources` (used by all `/v1/chunk/*/source/async` endpoints) are
  both typed `FileSourceRequest | HttpSourceRequest` only — an `S3Source` there is a **schema-level
  422** (Pydantic discriminated-union rejection before any policy check ever runs), not a
  disableable policy toggle.
- `BatchConvertSourcesRequest.sources` (used by `/v1/convert/source/batch`) is the only source
  union in this version that includes `S3SourceRequest`. Despite the name and the fact that a
  prior version of this doc described client-side batch/sharding as retired, `/source/batch` is
  still a single-request, async, `task_id`-pollable call through the same orchestrator as
  `/source/async` for a single source — nothing else about the submit/poll/resolve flow differs.
  `submit_source` uses this endpoint for exactly that reason.
- **There is no batch equivalent for chunk endpoints in docling-serve v1.29.0** — `app.py` only
  registers `/v1/chunk/{hybrid,hierarchical}/{source,file}/async` (and their sync counterparts),
  no `/v1/chunk/*/source/batch`. So a chunk submission can never hand docling-serve an S3
  *source* reference in this version, full stop. `submit_chunk_source` therefore has the parsing
  service read the replay-cached `DoclingDocument` JSON itself (a small extra read against its
  own S3-compatible storage, not through docling-serve) and embeds it as a base64
  `FileSourceRequest` — the *target* side still goes to `S3Target`, since
  `/v1/chunk/hybrid/source/async` (the JSON-body variant) is the only chunk endpoint whose
  `target` field accepts anything but inbody/zip; the `/file/async` multipart variant hard-codes
  `target` to inbody-or-zip only, so it couldn't have been used here either way. The bytes
  crossing the wire are the *converted* JSON, not the original PDF, so this doesn't reintroduce
  the large-payload problem this epic set out to avoid — it's meaningfully smaller for the vast
  majority of documents (no rasterized page images unless image export mode is enabled).

`S3Target`'s `key_prefix` is honored, but the exact written key underneath it is not fully
predictable: `docling_jobkit.convert.results_processor.ResultsProcessor` nests the artifact
under a format-type subfolder (`json/`, `chunks/`, etc.) plus a filename derived from the
source's own name — and on some docling-jobkit versions, an additional hash-subdirectory with
no API knob to disable (see `[[feedback_docling_jobkit_hash_path]]` and git history on
`router.py`, commit `d4a7c24`, for the same problem hit previously by the now-retired batch
endpoint). The parsing service never guesses the key: `_discover_s3_result_key` (`router.py`)
recursively lists the scratch prefix, filters by the expected file suffix
(`.json` for convert, `.chunks.jsonl` for chunk), and sorts by basename for determinism.

For chunk submissions specifically, `convert_options.to_formats: []` is set to suppress the
incidental json/md/html/text/doctags uploads `ResultsProcessor._upload_formats()` otherwise
also writes unconditionally alongside the chunks file — without it, the scratch prefix would
also collect files the discovery listing has to filter past.

### Hybrid chunking

Chunking is a fully decoupled stage from conversion in the parsing service (Epic 21 §21.8) —
it happens after `/v1/parse/resolve` has written the converted `DoclingDocument` to its
replay cache, via a separate `/v1/chunk/submit` call. `/v1/chunk/submit` reads the replay-cached
JSON itself (`chunk_submit_endpoint`, `router.py`) and hands docling-serve the bytes inline —
no chunk source endpoint in this docling-serve version accepts an S3 reference (see above), so
this is not a choice made for symmetry with conversion, it's the only option:

```
POST /v1/chunk/hybrid/source/async
Content-Type: application/json

{
  "convert_options": {"to_formats": []},
  "sources": [{"kind": "file", "base64_string": "<base64 DoclingDocument JSON>", "filename": "doc.json"}],
  "target": {"kind": "s3", "endpoint": ..., "bucket": ..., "key_prefix": "docling-out/chunk/{obj_id}/", ...}
}
```

`POST /v1/chunk/hybrid/file/async` (multipart form, not JSON body) also exists and is used only
by callers with no replay cache to read from (e.g.
`experiments/eval/runners/retrieval_eval.py`) — it's a distinct, more restricted endpoint from
the JSON-body `/source/async` variant used above: its `target` is a plain form field limited to
`InBodyTarget`/`ZipTarget`, no `S3Target`, so it could never have been used for the production
path regardless of the source-side fix.

Both return `{"task_id": "<uuid>"}`. The multipart/inbody path fetches its result with
`GET /v1/result/{task_id}` → `{"chunks": [...]}`; the production path (S3 target, inline base64
source) discovers its result via listing instead (see above) — the chunk rows in the
`.chunks.jsonl` file are the same `ChunkedDocumentResultItem` schema either way.

`target` for chunk endpoints defaults to in-body (`InBodyTarget()`) if omitted — unlike
convert's `target`, that default isn't policy-configurable via `resolve_default_target`, but
an explicit `S3Target` in the request body is still honored (confirmed via
`BaseChunkDocumentsRequest.target: TargetRequest = InBodyTarget()`, a plain overridable field,
not a server-enforced override).

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
