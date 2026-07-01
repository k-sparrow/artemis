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
| `POST` | `/v1/parse` | Parse a document; returns artifact `BlobRef` | `200 BlobRef` |
| `GET` | `/health` | Readiness probe (checks Docling reachability) | `200` or `503` |

---

## Parse Request

`POST /v1/parse` is a `multipart/form-data` request. Supply exactly one of `file` or
`source_ref`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | `UploadFile` | one of | Inline file bytes — used for direct uploads |
| `source_ref` | string (JSON) | one of | JSON-encoded `BlobRef {"bucket", "key"}` — used by the controller worker (claim-check) |
| `filename` | string | no | Display filename; inferred from `file.filename` if omitted |
| `content_type` | string | no | MIME type; inferred if omitted |
| `metadata` | string (JSON dict) | no | Extra key/value pairs; must include `"obj_id"` (the deterministic object UUID) |

Example form fields sent by the controller worker:
```
source_ref  = {"bucket": "artemis", "key": "550e8400.../3e4a5b6c..."}
filename    = "report.pdf"
content_type = "application/pdf"
metadata    = {"obj_id": "3e4a5b6c-..."}
```

---

## Parse Flow

1. Materialise document bytes in memory:
   - From `source_ref`: read from MinIO at `source_ref.bucket/source_ref.key`
   - From `file`: read from the multipart upload
2. Call `POST http://docling-serve:5001/v1/convert/file` (multipart/form-data)
3. Receive `DoclingDocument` JSON from Docling Serve
4. Extract from `DoclingDocument`:
   - `pages[]`: page-numbered Markdown blocks
   - `chunks[]`: semantic chunks with `DocItemLabel` type annotation
5. Write raw `DoclingDocument` JSON to the `docling-replay` bucket at key `replay/{obj_id}.json` (private replay cache)
6. Write `ParseArtifact` JSON to the `parsed-chunks` bucket at key `parse/{obj_id}.json`
7. Return `BlobRef {bucket: "parsed-chunks", key: "parse/{obj_id}.json"}`

The controller worker's `fetch_and_parse` task receives this `BlobRef` and passes it
directly to the indexing service as `artifact_ref`.

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

The raw `DoclingDocument` JSON is written to the `docling-replay` bucket:
```
replay/{obj_id}.json
```

This is private to the parsing service — no other service reads from this bucket.

Use case: if indexing parameters change (chunk size, pipeline type) but the document is
unchanged, the controller worker can trigger a replay that reads from this cache instead
of re-parsing, saving Docling compute time. This feature is not yet exposed via API.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `DOCLING_SERVE_URI` | *(required)* | |
| `S3_ENDPOINT` | *(required)* | |
| `S3_SECURE` | *(required)* | Set to `true` when `S3_ENDPOINT` uses HTTPS |
| `S3_ACCESS_KEY` | *(required)* | |
| `S3_SECRET_KEY` | *(required)* | |
| `PARSED_ARTIFACTS_BUCKET` | `parsed-chunks` | |
| `REPLAY_CACHE_BUCKET` | `docling-replay` | |
| `LOADER_TYPE` | `DOCLING` | `DOCLING` or `PYMUPDF4LLM` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-parsing` | |
