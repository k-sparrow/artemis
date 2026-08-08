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

Both convert and chunk results come back via an S3Target rather than inline in the HTTP
response (Epic 21 §21.9) — docling-serve writes the artifact directly into this service's own
bucket instead of returning it in the task-result body, so a large `DoclingDocument` JSON never
has to cross an HTTP response. `resolve_endpoint`/`chunk_finalize_endpoint` discover the exact
written key via a recursive listing (`_discover_s3_result_key`) rather than guessing it —
docling-jobkit nests the artifact under path segments (a format-type folder always, and on some
versions an extra hash-subdirectory) with no API knob to disable. `PresignedUrlTarget` was
considered and rejected: it doesn't work against locally-deployed S3 clusters (confirmed
directly in code in a prior session) and is explicitly disallowed for chunk endpoints regardless
(`docling-serve` returns 422 — `"presigned_url target is not supported for chunk endpoints"`),
so plain `S3Target` is used for both convert and chunk rather than mixing target kinds.

---

## Async Parse + Chunk Flow

The controller worker drives a scatter-gather pipeline across four long-running phases so no
Celery thread is blocked during conversion or chunking.

```
submit_endpoint         → Docling Serve /v1/convert/file/async (or .../source/async)
                               ↓ polling via status_endpoint
resolve_endpoint        → download conversion result
                        → write replay cache (raw DoclingDocument JSON)
                        → derive + cache pages (Markdown parents)
                               ↓
chunk_submit_endpoint   → Docling Serve /v1/chunk/hybrid/source/async
                          (reads the replay cache directly from S3 — no bytes
                          re-uploaded here, mirrors submit's source_ref path)
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
and TODOs.md Epic 21). A prior version of this service did its own client-side PDF sharding
via a `/v1/convert/source/batch` S3 workflow — that path was retired outright once the Ray
engine made it unnecessary, not kept as a fallback.

### Submit (`POST /v1/parse/submit`)

Form fields identical to `POST /v1/parse`. Logic:

1. Materialise document bytes: inline `file` bytes are read directly and forwarded to
   `POST /v1/convert/file/async`; `source_ref` claim-check inputs are handed to docling-serve
   as an S3 source via `POST /v1/convert/source/async` — docling-serve fetches the object
   directly from MinIO/S3 itself, so this service never downloads the bytes into its own
   memory (Epic 21 §21.3).
2. Return `{parsing_task_id, obj_id}`.

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

Request body: `{parsing_task_id, obj_id}`.

1. `submit_endpoint` pointed docling-serve's `target` at
   `REPLAY_CACHE_BUCKET/docling-out/convert/{obj_id}/` (an `S3Target`, not `inbody`) — the
   conversion result already landed there server-side by the time status reported `"success"`.
2. Discover the exact written key: recursively list `docling-out/convert/{obj_id}/`, filter to
   keys ending `.json`, sort by basename, take the sole match (`_discover_s3_result_key`;
   `502` if none found — docling-serve reported success but produced nothing discoverable).
3. Read those bytes once and parse into a `DoclingDocument`.
4. Copy the same bytes to the stable replay cache key: `REPLAY_CACHE_BUCKET/replay/{obj_id}.json`
   — downstream consumers (chunk submit, future re-chunk/citation tooling) reference this fixed
   key, never the scratch prefix. Delete the scratch key.
5. Derive pages via `build_pages()` (`lib/artifact.py`, which calls `split_pages()` in
   `lib/core/adapters/loaders/docling.py`): `dl_doc.export_to_markdown(page_break_placeholder=
   "<!-- ARTEMIS_PAGE_BREAK -->")` once, then splits on the placeholder to produce
   `(page_no, markdown)` tuples.
6. Write pages cache: `REPLAY_CACHE_BUCKET/pages/{obj_id}.json`.
7. Return `{obj_id}` — no chunk job submitted here.

### Chunk Submit (`POST /v1/chunk/submit`)

Request body: `{obj_id}`. Must only be called after `/v1/parse/resolve` has written the
replay cache for this `obj_id`.

1. `POST /v1/chunk/hybrid/source/async` pointing at `REPLAY_CACHE_BUCKET/replay/{obj_id}.json`
   as an S3 source — docling-serve fetches the JSON directly, this service never re-reads or
   re-uploads it — with `target` pointed at `REPLAY_CACHE_BUCKET/docling-out/chunk/{obj_id}/`
   (`S3Target`) and `convert_options.to_formats: []` to suppress the incidental json/md/html/etc.
   uploads `ResultsProcessor` would otherwise also write alongside the chunks file.
2. Return `{chunking_task_id, obj_id}`.

### Chunk Finalize (`POST /v1/chunk/finalize`)

Request body: `{chunking_task_id, obj_id, metadata}`. Must only be called after
`GET /v1/chunk/status/{chunking_task_id}` returns `"success"`.

1. Discover the written chunks key: recursively list `docling-out/chunk/{obj_id}/`, filter to
   keys ending `.chunks.jsonl`, sort by basename, take the sole match. `502` if none found.
2. Read and parse the newline-delimited JSON — one `ChunkedDocumentResultItem`-shaped dict per
   line, the same schema as the old inline `chunks` list. Delete the scratch key.
3. Read `REPLAY_CACHE_BUCKET/pages/{obj_id}.json` → cached `Page[]` (never re-fetches or
   re-parses the `DoclingDocument`).
4. Map chunks → `ParsedChunk[]`, build `ParseArtifact` from chunks + cached pages.
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

Written by `resolve_endpoint` (async flow) and by `POST /v1/parse` (sync flow). Read back
by `chunk_submit_endpoint`, which hands docling-serve this S3 location directly rather than
re-uploading the JSON — chunking never re-fetches it into this service's own memory.

Derived page-level Markdown parents are cached alongside it, in the same bucket:
```
pages/{obj_id}.json
```

Written once by `resolve_endpoint` (right after conversion, while the `DoclingDocument` is
already in memory) and read back by `chunk_finalize_endpoint` to build the final
`ParseArtifact` — chunk finalization never re-derives pages or re-parses the `DoclingDocument`.

Both keys are private to the parsing service; no other service reads from this bucket.

Two further prefixes in the same bucket are purely transient — docling-serve's `S3Target`
scratch space, discovered and deleted within a single request (Epic 21 §21.9), never
referenced after that request completes:
```
docling-out/convert/{obj_id}/   — conversion output, before it's copied to replay_key()
docling-out/chunk/{obj_id}/     — chunk output, before it's read into the final ParseArtifact
```

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
| `LOADER_TYPE` | `DOCLING` | `DOCLING` or `PYMUPDF4LLM` (dev fallback, no GPU) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-parsing` | |

`DOCLING_RESOLVE_TIMEOUT` / `DOCLING_FINALIZE_TIMEOUT` were removed (Epic 21 §21.9) — resolve
and chunk finalize no longer call docling-serve at all (S3 listing/read/write instead of an
inline HTTP fetch), so there's no docling-serve HTTP call left for those settings to bound.
