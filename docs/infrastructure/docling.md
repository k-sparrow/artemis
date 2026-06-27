# Docling Serve

**Version:** ds4sd/docling-serve:v1.24.0-cu128  
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

The parsing service calls Docling Serve and extracts from the `DoclingDocument`:
- `pages[]` — page-numbered Markdown blocks (for parent-page retrieval)
- `chunks[]` — semantic chunks with `DocItemLabel` type (for embedding)

---

## API Usage

The parsing service calls the synchronous conversion endpoint:

```
POST http://docling-serve:5001/v1/convert/file
Content-Type: multipart/form-data

file=@document.pdf
```

Docling Serve returns a `DoclingDocument` JSON when the conversion completes.

**Async mode:** For documents that exceed `DOCLING_SERVE_MAX_SYNC_WAIT` (12 seconds by
default), Docling Serve switches to async mode and returns a `task_id` instead of the
result. The current parsing service implementation does not handle this case — it waits
synchronously and times out on large documents.

This is a known limitation tracked as **Epic 18** (Async Docling Parsing for Large
Documents). The stopgap is `HTTPX_TIMEOUT=86400` and `consumer_timeout=86400000ms` to
allow up to 24 hours for heavy PDFs. This works for typical enterprise PDFs (up to ~500
pages with GPU) but is not a long-term solution.

---

## Table Handling

The parsing service controls how tables are chunked:

- **SEMI_STRUCTURED pipeline:** `split_tables=False` — tables are kept whole as single
  chunks (correct for multi-vector RAG; the table is stored as a unit and summarised)
- **SIMPLE pipeline:** `split_tables=True` — tables are split at the splitter's chunk
  boundary (trades coherence for size limits)

`DocItemLabel.TABLE` is detected by `MetaExtractor` and assigned `ChunkType.TABLE` in the
`ParseArtifact.chunks[]` output.

---

## CUDA Version

The image `v1.24.0-cu128` requires CUDA 12.8. If your GPU does not support CUDA 12.8,
use a compatible image version or run Docling Serve on CPU (significantly slower, not
recommended for production).

---

## Replay Cache

The parsing service writes the raw `DoclingDocument` JSON to MinIO:
```
docling-replay/{namespace_id}/{obj_id}.json
```

This is private to the parsing service — the bucket and key are not in any inter-service
contract. The cache allows re-running the indexing pipeline without re-parsing the document
(useful when indexing parameters change but the document content is unchanged).

---

## Health Check

The parsing service's readiness probe includes a Docling reachability check:

```bash
# From inside Docker network
curl http://docling-serve:5001/health
```

If Docling Serve is down, the parsing service returns `503 Service Unavailable` on
`POST /v1/parse` immediately (circuit breaker opens after 3 failures).
