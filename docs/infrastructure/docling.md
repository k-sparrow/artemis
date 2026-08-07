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

### Hybrid chunking

```
POST /v1/chunk/hybrid/file/async
Content-Type: multipart/form-data

files=("doc.json", <DoclingDocument bytes>, "application/json")
```

Returns `{"task_id": "<uuid>"}`. Fetch result with `GET /v1/result/{task_id}` →
`{"chunks": [...]}`.

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
