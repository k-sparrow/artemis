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
| `POST` | `/v1/parse` | Parse a document synchronously; returns artifact `BlobRef` | `200 BlobRef` |
| `POST` | `/v1/parse/submit` | Submit a conversion job to Docling Serve; returns immediately | `200 SubmitResult` |
| `GET`  | `/v1/parse/status/{task_id}` | Single non-blocking status check (conversion or chunk task) | `200 ParseStatus` |
| `POST` | `/v1/parse/resolve` | Download conversion results, concatenate shards, submit chunk job | `200 ResolveResult` |
| `POST` | `/v1/parse/finalize` | Fetch chunk result, build `ParseArtifact`, write to MinIO | `200 BlobRef` |
| `GET`  | `/health` | Readiness probe (checks Docling reachability) | `200` or `503` |

`POST /v1/parse` is the legacy synchronous endpoint retained for backward compatibility.
The controller worker uses the async chain (`submit` → `status` → `resolve` → `status` → `finalize`).

---

## Async Parse Flow

The controller worker drives a scatter-gather pipeline across two long-running phases so no
Celery thread is blocked during conversion or chunking.

```
submit_endpoint        → Docling Serve /v1/convert/file/async      (small docs)
                       → Docling Serve /v1/convert/source/batch     (large PDFs)
                              ↓ polling via status_endpoint
resolve_endpoint       → download results from MinIO scratch
                       → DoclingDocument.concatenate() (batch mode)
                       → write replay cache
                       → Docling Serve /v1/chunk/hybrid/file/async
                              ↓ polling via status_endpoint
finalize_endpoint      → fetch chunk result
                       → build ParseArtifact from chunks + replay cache
                       → write to parsed-chunks bucket
                       → return BlobRef
```

### Submit (`POST /v1/parse/submit`)

Form fields identical to `POST /v1/parse`. Logic:

1. Materialise document bytes (inline `file` or `source_ref` claim-check).
2. If the document is a PDF larger than `DOCLING_SHARD_TRIGGER_PAGES` pages:
   - Split into `DOCLING_SHARD_PAGE_LIMIT`-page shards via `pypdf`.
   - Upload shards to `DOCLING_SCRATCH_BUCKET` at `{obj_id}/pages/shard-{n:04d}.pdf`.
   - `POST /v1/convert/source/batch` (S3Source → S3Target) → `parsing_task_id`.
   - Return `mode=batch`, `shard_count=N`, `scratch_prefix={obj_id}/`.
3. Otherwise: `POST /v1/convert/file/async` → `parsing_task_id`. Return `mode=single`.

### Status (`GET /v1/parse/status/{task_id}`)

Proxies `GET /v1/status/poll/{task_id}?wait=0` to Docling Serve. Works for both
conversion and chunk `task_id`s — Docling Serve uses a unified task namespace.

Returns `ParseStatus`:

```json
{"status": "processing" | "success" | "failure", "num_processed": null, "num_total": null, "error_message": null}
```

Docling Serve's `ConversionStatus` is normalised as follows:

| Docling Serve status | `ParseStatus.status` | Notes |
|---|---|---|
| `pending`, `started` | `processing` | No distinction exposed — LOCAL engine provides no progress |
| `success` | `success` | |
| `failure` | `failure` | `error_message` = server error string |
| `partial_success` | `failure` | `error_message = "partial_success: <server_error>"` — some shards failed; incomplete S3 results; treat as failure and retry whole batch |
| `skipped` | `failure` | `error_message = "skipped: <server_error>"` — unexpected in this flow |

> **Note on progress:** The Docling Serve LOCAL engine does not populate `task_meta`
> (`num_processed`/`num_total`). Both fields are always `null`. Progress tracking requires
> the RAY orchestrator backend.

### Resolve (`POST /v1/parse/resolve`)

Request body: `{parsing_task_id, mode, obj_id, scratch_prefix, shard_count}`.

1. **Single mode:** `GET /v1/result/{parsing_task_id}` → `DoclingDocument`.
2. **Batch mode:** list objects under `{obj_id}/results/` recursively, filter `.json`,
   sort by basename, download each, call `DoclingDocument.concatenate(shards)`.
3. Write replay cache: `REPLAY_CACHE_BUCKET/replay/{obj_id}.json`.
4. `POST /v1/chunk/hybrid/file/async` with `doc.json` → `chunking_task_id`.
5. Delete scratch objects under `{obj_id}/` (batch mode only, best-effort).
6. Return `{chunking_task_id, obj_id}`.

### Finalize (`POST /v1/parse/finalize`)

Request body: `{chunking_task_id, obj_id, metadata}`.

1. `GET /v1/result/{chunking_task_id}` → chunk list.
2. Read `REPLAY_CACHE_BUCKET/replay/{obj_id}.json` → `DoclingDocument` (for page extraction).
3. Export `DoclingDocument` to Markdown via `split_pages()` (`lib/core/adapters/loaders/docling.py`):
   calls `dl_doc.export_to_markdown(page_break_placeholder="<!-- ARTEMIS_PAGE_BREAK -->")` once,
   then splits on the placeholder to produce `(page_no, markdown)` tuples.
4. Map chunks → `ParsedChunk[]`, build `ParseArtifact`.
5. Write to `PARSED_ARTIFACTS_BUCKET/parse/{obj_id}.json`.
6. Return `BlobRef`.

---

## ParseArtifact Schema

```json
{
  "pages": [
    {"page_no": 1, "markdown": "# Section Title\n\nParagraph text..."}
  ],
  "chunks": [
    {"page_content": "Paragraph text...", "source": "document.pdf", "type": "TEXT", "page_no": 1},
    {"page_content": "| Col A | Col B |\n|-------|-------|\n| v1 | v2 |", "source": "document.pdf", "type": "TABLE", "page_no": 2}
  ]
}
```

`type` mirrors `DocItemLabel` values: `TEXT`, `TABLE`, `SECTION_HEADER`, `CAPTION`,
`LIST_ITEM`, `PAGE_HEADER`, `PAGE_FOOTER`, `FOOTNOTE`, `FORMULA`, `PICTURE`, `CODE`, `UNKNOWN`.

`dl_meta` is explicitly not included — the `ParseArtifact` schema is stable regardless of
Docling version changes.

---

## Pipeline Type Interaction

| Pipeline type | Table handling | Chunk granularity |
|---------------|----------------|-------------------|
| `SIMPLE` | `split_tables=True` — tables split at chunk boundaries | Token-count-bounded chunks |
| `SEMI_STRUCTURED` | `split_tables=False` — tables kept whole | Semantic chunks matching Docling's document structure |

---

## Replay Cache

The raw `DoclingDocument` JSON is written to the `docling-replay` bucket:
```
replay/{obj_id}.json
```

Written by `resolve_endpoint` (async flow) and by `POST /v1/parse` (sync flow).
Read back by `finalize_endpoint` for page extraction — avoids re-fetching or passing
the full document through the response body.

This bucket is private to the parsing service; no other service reads from it.

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
| `DOCLING_SCRATCH_BUCKET` | `docling-scratch` | Shard staging + batch results |
| `DOCLING_SHARD_PAGE_LIMIT` | `400` | Pages per shard |
| `DOCLING_SHARD_TRIGGER_PAGES` | `400` | Shard only if PDF exceeds this |
| `DOCLING_STATUS_TIMEOUT` | `30.0` | Seconds, for status proxy call |
| `DOCLING_RESOLVE_TIMEOUT` | `120.0` | Seconds, for resolve (downloads + chunk submit) |
| `DOCLING_FINALIZE_TIMEOUT` | `60.0` | Seconds, for finalize (chunk fetch + artifact write) |
| `LOADER_TYPE` | `DOCLING` | `DOCLING` or `PYMUPDF4LLM` (dev fallback, no GPU) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-parsing` | |
