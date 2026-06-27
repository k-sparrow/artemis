# Parsing Service

**Port:** 10001 (internal), 10003 (host mapping)  
**Framework:** FastAPI + httpx + aiobotocore  
**Source:** `src/backend/parsing/`

The parsing service is a stateless bridge between raw documents and the indexing pipeline.
It accepts a file reference (claim-check), calls Docling Serve, and writes a structured
`ParseArtifact` to MinIO. It never sends document bytes over HTTP to another service.

---

## Endpoints

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/v1/parse` | Parse a document; returns artifact key | `202 ParseResponse` |
| `GET` | `/health` | Readiness probe (checks Docling reachability) | `200` or `503` |

---

## Parse Request

```json
{
  "source_ref": {
    "bucket": "artemis",
    "key": "<namespace_id>/<obj_id>"
  },
  "pipeline_type": "simple",
  "namespace_id": "<uuid>",
  "obj_id": "<uuid>"
}
```

`source_ref` is a `BlobRef` (claim-check). The parsing service reads the file bytes from
MinIO using this reference — no file bytes come in the HTTP request body.

---

## Parse Flow

1. Read file bytes from MinIO at `source_ref.bucket/source_ref.key`
2. Write bytes to a temporary path on disk
3. Call `POST http://docling-serve:5001/v1/convert/file` (multipart/form-data)
4. Receive `DoclingDocument` JSON from Docling Serve
5. Write raw `DoclingDocument` to `docling-replay/{namespace_id}/{obj_id}.json` (replay cache)
6. Extract from `DoclingDocument`:
   - `pages[]`: page-numbered Markdown blocks
   - `chunks[]`: semantic chunks with `DocItemLabel` type annotation
7. Write `ParseArtifact` JSON to `parsed-chunks/{uuid4}.json`
8. Delete temp file on disk
9. Return `{out_key: "<uuid4>.json"}`

The caller (controller worker's `fetch_and_parse` task) receives `out_key` and passes it
to the indexing service as `BlobRef {bucket: "parsed-chunks", key: "<uuid4>.json"}`.

---

## ParseArtifact Schema

```json
{
  "pages": [
    {
      "page_no": 1,
      "markdown": "# Section Title\n\nParagraph text..."
    }
  ],
  "chunks": [
    {
      "page_content": "Paragraph text...",
      "source": "document.pdf",
      "type": "TEXT",
      "page_no": 1
    },
    {
      "page_content": "| Col A | Col B |\n|-------|-------|\n| v1 | v2 |",
      "source": "document.pdf",
      "type": "TABLE",
      "page_no": 2
    }
  ]
}
```

`type` mirrors `DocItemLabel` values: `TEXT`, `TABLE`, `SECTION_HEADER`, `CAPTION`,
`LIST_ITEM`, `PAGE_HEADER`, `PAGE_FOOTER`, `FOOTNOTE`, `FORMULA`, `PICTURE`, `CODE`,
`UNKNOWN`.

`dl_meta` is explicitly not included. Docling's internal layout metadata is not part of the
inter-service contract — the `ParseArtifact` schema is stable regardless of Docling version changes.

---

## Pipeline Type Interaction

| Pipeline type | Table handling | Chunk granularity |
|---------------|----------------|-------------------|
| `SIMPLE` | `split_tables=True` — tables split at chunk boundaries | Token-count-bounded chunks |
| `SEMI_STRUCTURED` | `split_tables=False` — tables kept whole | Semantic chunks matching Docling's document structure |

The parsing service passes `pipeline_type` to the Docling loader to set `split_tables`.

---

## Loader Type

Controlled by `LOADER_TYPE` env var:

| Value | Loader | Notes |
|-------|--------|-------|
| `DOCLING` (default) | `DoclingAPIServeLoader` | Calls Docling Serve HTTP API |
| `PYMUPDF4LLM` | `PyMuPDF4LLMLoader` | CPU-only, no Docling dependency; no structured types |

`PYMUPDF4LLM` is intended for dev environments without GPU. It produces flat text chunks
without `DocItemLabel` types (all become `UNKNOWN`). The indexing pipeline handles this gracefully.

---

## Replay Cache

The raw `DoclingDocument` JSON is written to `docling-replay/` bucket:
```
docling-replay/{namespace_id}/{obj_id}.json
```

This is private to the parsing service — no other service reads from this bucket.

Use case: if indexing parameters change (chunk size, pipeline type) but the document is
unchanged, the controller worker can trigger a replay that reads from this cache instead
of re-parsing, saving Docling compute time. This feature is not yet exposed via API.

---

## Circuit Breaker

If Docling Serve returns errors, the parsing service's internal `DoclingBreaker` opens
after 3 consecutive failures. Subsequent calls fail immediately with
`HTTP_503_SERVICE_UNAVAILABLE` until the breaker resets (60 seconds). The controller
worker's `CircuitBreakerError` autoretry handles this case.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `DOCLING_SERVE_URI` | `http://docling-serve:5001` | |
| `S3_ENDPOINT_URL` | `http://minio:9000` | |
| `S3_ACCESS_KEY` | `minioadmin` | |
| `S3_SECRET_KEY` | `minioadmin` | |
| `PARSED_ARTIFACTS_BUCKET` | `parsed-chunks` | |
| `REPLAY_CACHE_BUCKET` | `docling-replay` | |
| `LOADER_TYPE` | `DOCLING` | `DOCLING` or `PYMUPDF4LLM` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-parsing` | |
