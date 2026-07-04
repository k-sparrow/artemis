# Docling Serve

**Version:** ghcr.io/docling-project/docling-serve-cu128:v1.24.0  
**Role:** GPU-accelerated document parsing. Converts PDFs, DOCX, HTML, and other formats
into structured output: page-delimited Markdown and semantic chunks with content type
labels (TEXT, TABLE, SECTION_HEADER, etc.).

**Compose profile:** `ai`  
**Port:** 5001 (internal only, not exposed to host)

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

### Batch conversion (large PDFs — S3Source → S3Target)

Used when a PDF exceeds `DOCLING_SHARD_TRIGGER_PAGES` pages. The parsing service
pre-splits the PDF into shards, uploads them to the `docling-scratch` MinIO bucket, then
submits a batch job:

```
POST /v1/convert/source/batch
Content-Type: application/json

{
  "options": {"to_formats": ["json"]},
  "sources": [{"kind": "s3", "endpoint": "...", "bucket": "docling-scratch", "key_prefix": "{obj_id}/pages/"}],
  "target":  {"kind": "s3", "endpoint": "...", "bucket": "docling-scratch", "key_prefix": "{obj_id}/results/"}
}
```

Returns `{"task_id": "<uuid>"}` immediately. Docling Serve lists all objects under
`key_prefix` and converts each in sequence (LOCAL engine) or in parallel (RAY engine).

#### S3Target hash path behaviour

`GET /v1/result/{task_id}` for a batch job returns **counts only** — no file paths and no
presigned URLs. The actual result JSONs are written to the scratch bucket, but
`docling_jobkit` (Docling Serve's internal job library) appends a 12-char SHA-256 hash of
each source URI between `dst_prefix` and the shard filename:

```
{dst_prefix}{sha256(source_uri)[:12]}/{shard_basename}.json
```

There is no API knob to disable this. The parsing service absorbs it by listing
`{obj_id}/results/` **recursively** and sorting results by basename — the hash directory
level is transparent to the caller.

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
| `partial_success` | Terminal: some documents in the batch succeeded, some failed |
| `skipped` | Terminal: document(s) skipped |

The LOCAL engine (default) does not populate `task_meta.num_processed` / `task_meta.num_total`.
Progress tracking is only available with the RAY orchestrator backend.

The LOCAL engine status transitions: `pending` → `started` → `success` | `failure`.
`started` is set synchronously when the worker dequeues the task, before the conversion
thread is spawned.

---

## Table Handling

- **SEMI_STRUCTURED pipeline:** `split_tables=False` — tables kept whole as single chunks
- **SIMPLE pipeline:** `split_tables=True` — tables split at the chunk boundary

---

## CUDA Version

The image `docling-serve-cu128:v1.24.0` requires CUDA 12.8. If your GPU does not support
CUDA 12.8, use a compatible image version or run Docling Serve on CPU (significantly
slower, not recommended for production).

---

## Server-side timeout

`DOCLING_SERVE_MAX_SYNC_WAIT` is set to `12` seconds in compose (the default). The
parsing service uses only `/async` and `/batch` endpoints — it never calls the synchronous
`/v1/convert/file` endpoint, so the sync timeout is irrelevant for production traffic.

---

## Health Check

The parsing service's readiness probe checks Docling reachability:

```bash
curl http://docling-serve:5001/health
```

If Docling Serve is down, the parsing service returns `503 Service Unavailable` on
`POST /v1/parse/submit` immediately (circuit breaker opens after 3 failures).
