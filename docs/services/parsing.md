# Parsing Service

**Port:** 10001 (internal), 10003 (host mapping)  
**Framework:** FastAPI + httpx + aiobotocore  
**Source:** `src/backend/parsing/`

The parsing service is a stateless bridge between raw documents and the indexing pipeline.
It accepts a file reference (claim-check), calls Docling Serve, and writes a structured
`ParseArtifact` to MinIO. Input dispatch branches on submission mode: inline `file` bytes are
forwarded to Docling Serve as multipart; `source_ref` (S3 claim-check) inputs are dispatched
S3-direct — this service only verifies the referenced object exists, never reads it, and
passes the bucket/key straight through to Docling Serve, which fetches it itself. S3-direct
submission requires Docling Serve's Ray-serde patch (`tools/oci/images/docling`) — see
[docs/infrastructure/docling.md](../infrastructure/docling.md) for the full investigation and
the upstream bug this patch fixes.

---

## Endpoints

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/v1/parse` | Parse a document synchronously; returns artifact `BlobRef` | `200 BlobRef` |
| `POST` | `/v1/parse/submit` | Submit a conversion job to Docling Serve; returns immediately | `200 SubmitResult` |
| `GET`  | `/v1/parse/status/{task_id}` | Single non-blocking status check (conversion or chunk task) | `200 ParseStatus` |
| `POST` | `/v1/parse/resolve` | Download conversion result; cache replay JSON + derived pages | `200 ResolveResult` |
| `POST` | `/v1/chunk/submit` | Submit the replay-cached document for hybrid chunking; returns immediately | `200 ChunkSubmitResult` |
| `GET`  | `/v1/chunk/status/{task_id}` | Single non-blocking status check (chunk task) | `200 ParseStatus` |
| `POST` | `/v1/chunk/finalize` | Fetch chunk result, merge with cached pages, build `ParseArtifact`, write to MinIO | `200 BlobRef` |
| `GET`  | `/health` | Readiness probe (checks Docling reachability) | `200` or `503` |

`POST /v1/parse` is the legacy synchronous endpoint retained for backward compatibility.
The controller worker uses the async chain (`submit` → `status` → `resolve` → `chunk/submit`
→ `chunk/status` → `chunk/finalize`).

Chunking is a fully decoupled stage from conversion (Epic 21): `/v1/parse/resolve` only
downloads and caches the conversion result — it never submits a chunk job. `/v1/chunk/*`
owns that separately, so the two stages can be orchestrated (retried, scheduled) independently
by the controller worker. The `/v1/chunk/*` endpoints still live in this service rather than
the worker or the indexing service — this service remains the *sole* place in Artemis that
imports Docling types or knows docling-serve's API surface (the anti-corruption-layer
property), it just no longer bundles chunking into the conversion request/response cycle.
Named `/v1/chunk/*` rather than nested under `/v1/parse/` so the path doesn't imply chunking
is owned by "parsing" as a concept — it isn't, this service just happens to be the sole
Docling adapter for now. A future direction (not started) is retiring this shim altogether
and having the controller worker call docling-serve directly, since most heavy lifting —
including page-slice fan-out — now happens server-side; see TODOs.md Epic 21.

---

## Async Parse + Chunk Flow

The controller worker drives a scatter-gather pipeline across four long-running phases so no
Celery thread is blocked during conversion or chunking.

```
submit_endpoint         → file: Docling Serve /v1/convert/file/async (inline)
                          source_ref: verify existence, then Docling Serve
                          /v1/convert/source/batch (S3-direct, S3 source + target)
                               ↓ polling via status_endpoint
resolve_endpoint        → file: download conversion result inline
                          source_ref: discover result under the S3Target
                          scratch prefix (recursive listing), read, delete
                        → write replay cache (raw DoclingDocument JSON)
                        → derive + cache pages (Markdown parents)
                               ↓
chunk_submit_endpoint   → reads replay cache from S3 itself, then
                          Docling Serve /v1/chunk/hybrid/file/async
                          (sent inline — no chunk endpoint takes an S3 source)
                               ↓ polling via chunk_status_endpoint
chunk_finalize_endpoint → fetch chunk result
                        → read cached pages (never re-derives them)
                        → build ParseArtifact from chunks + pages
                        → write to parsed-chunks bucket
                        → return BlobRef
```

Every document takes this same single-submission path regardless of size — there is no
client-side splitting here. Docling Serve's Ray engine fans large PDFs out into page slices
and converts them concurrently across Ray actors **server-side**; this service has no
visibility into that and needs none (see [docs/infrastructure/docling.md](../infrastructure/docling.md)
and TODOs.md Epic 21). `/v1/convert/source/batch` is now used for `source_ref` submissions —
not for client-side PDF sharding (that was retired once the Ray engine made it unnecessary),
but because it's the only docling-serve endpoint whose schema accepts an S3 source at all. It
was tried once before and reverted for an unrelated reason (an upstream Ray-orchestrator bug
that broke every S3-source submission); that bug is now fixed via a patched image — see
[docs/infrastructure/docling.md](../infrastructure/docling.md).

### Submit (`POST /v1/parse/submit`)

Form fields identical to `POST /v1/parse`. Logic:

1. `file`: forwarded to `POST /v1/convert/file/async` as multipart, unchanged.
2. `source_ref`: this service verifies the referenced object exists
   (`blob_store(ref.bucket).aexists(ref.key)`) — 422 if not, so a stale reference fails fast
   instead of dispatching a job that would fail asynchronously and opaquely — then calls
   `POST /v1/convert/source/batch` with the bucket/key passed through as an S3 source and a
   per-`obj_id` scratch prefix as an S3 target. `/v1/convert/source/async` (the non-batch
   endpoint) still schema-rejects S3 sources; only the batch endpoint's schema accepts them,
   and its target has no inbody option, which is why the result comes back via S3Target
   instead of inline — see Resolve below.
3. Return `{parsing_task_id, obj_id, mode}` — `mode` (`"file"` | `"source"`) records which
   branch was taken so Resolve knows how to retrieve the result. The controller worker
   forwards this dict unmodified as Resolve's request body.

### Status (`GET /v1/parse/status/{task_id}`, `GET /v1/chunk/status/{task_id}`)

Both proxy `GET /v1/status/poll/{task_id}?wait=0` to Docling Serve — identical proxy logic,
since docling-serve uses one unified task namespace for every task type regardless of origin.
The two routes exist so polling a chunk task doesn't read as polling a parse task.

Returns `ParseStatus`:

```json
{"status": "processing" | "success" | "failure", "num_processed": null, "num_total": null, "error_message": null}
```

Docling Serve's `ConversionStatus` is normalised as follows:

| Docling Serve status | `ParseStatus.status` | Notes |
|---|---|---|
| `pending`, `started` | `processing` | No further distinction exposed |
| `success` | `success` | |
| `failure` | `failure` | `error_message` = server error string |
| `partial_success` | `failure` | `error_message = "partial_success: <server_error>"` — some page slices failed server-side (Ray engine fan-out); treat as failure and retry the whole document rather than proceed with a truncated result |
| `skipped` | `failure` | `error_message = "skipped: <server_error>"` — unexpected in this flow |

> **Note on progress:** `task_meta.num_processed` / `num_total` population under the Ray
> engine's page-slice fan-out isn't a documented Docling Serve contract. This service reads
> them opportunistically (never requires them) — don't build logic that depends on them being
> populated.

### Resolve (`POST /v1/parse/resolve`)

Request body: `{parsing_task_id, obj_id, mode}`.

1. `mode="file"`: `GET /v1/result/{parsing_task_id}` → `DoclingDocument`, as before.
   `mode="source"`: recursively list `REPLAY_CACHE_BUCKET` under the per-`obj_id` scratch
   prefix (`convert_scratch_prefix`) for the `.json` docling-serve's S3Target wrote — the
   nesting under that prefix isn't fully under our control, hence listing rather than guessing
   the exact key — read it, then delete the scratch object.
2. Write replay cache: `REPLAY_CACHE_BUCKET/replay/{obj_id}.json`.
3. Derive pages via `build_pages()` (`lib/artifact.py`, which calls `split_pages()` in
   `lib/core/adapters/loaders/docling.py`): `dl_doc.export_to_markdown(page_break_placeholder=
   "<!-- ARTEMIS_PAGE_BREAK -->")` once, then splits on the placeholder to produce
   `(page_no, markdown)` tuples.
4. Write pages cache: `REPLAY_CACHE_BUCKET/pages/{obj_id}.json`.
5. Return `{obj_id}` — no chunk job submitted here.

### Chunk Submit (`POST /v1/chunk/submit`)

Request body: `{obj_id}`. Must only be called after `/v1/parse/resolve` has written the
replay cache for this `obj_id`.

1. Read `REPLAY_CACHE_BUCKET/replay/{obj_id}.json` into memory — no chunk endpoint accepts an
   S3 source, so this service must fetch it itself.
2. `POST /v1/chunk/hybrid/file/async` with those bytes as multipart.
3. Return `{chunking_task_id, obj_id}`.

### Chunk Finalize (`POST /v1/chunk/finalize`)

Request body: `{chunking_task_id, obj_id, metadata}`. Must only be called after
`GET /v1/chunk/status/{chunking_task_id}` returns `"success"`.

1. `GET /v1/result/{chunking_task_id}` → chunk list.
2. Read `REPLAY_CACHE_BUCKET/pages/{obj_id}.json` → cached `Page[]` (never re-fetches or
   re-parses the `DoclingDocument`).
3. Map chunks → `ParsedChunk[]`, build `ParseArtifact` from chunks + cached pages.
4. Write to `PARSED_ARTIFACTS_BUCKET/parse/{obj_id}.json`.
5. Return `BlobRef`.

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

Written by `resolve_endpoint` (async flow) and by `POST /v1/parse` (sync flow). Read back
by `chunk_submit_endpoint`, which reads these bytes itself and submits them inline — no
docling-serve v1.29.0 endpoint accepts an S3 reference for chunk sources.

Derived page-level Markdown parents are cached alongside it, in the same bucket:
```
pages/{obj_id}.json
```

Written once by `resolve_endpoint` (right after conversion, while the `DoclingDocument` is
already in memory) and read back by `chunk_finalize_endpoint` to build the final
`ParseArtifact` — chunk finalization never re-derives pages or re-parses the `DoclingDocument`.

Both keys are private to the parsing service; no other service reads from this bucket.

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
| `DOCLING_STATUS_TIMEOUT` | `30.0` | Seconds, for status proxy calls (parse and chunk) |
| `DOCLING_RESOLVE_TIMEOUT` | `120.0` | Seconds, for resolve (conversion result download + replay/pages cache write) |
| `DOCLING_FINALIZE_TIMEOUT` | `60.0` | Seconds, for chunk finalize (chunk result fetch + artifact write) |
| `LOADER_TYPE` | `DOCLING` | `DOCLING` or `PYMUPDF4LLM` (dev fallback, no GPU) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-parsing` | |
