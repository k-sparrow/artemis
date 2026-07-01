# Indexing Service

**Port:** 10000  
**Framework:** FastAPI + LangChain + async SQLAlchemy + aiobotocore  
**Source:** `src/backend/indexing/`

The indexing service embeds document chunks, stores them in Qdrant, and handles retrieval
queries. It is stateless with respect to the documents — all state lives in Qdrant and the
LangChain RecordManager (PostgreSQL).

---

## Endpoints

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/ingest` | Index a parsed artifact | `200 UpsertResponse` |
| `DELETE` | `/ingest` | Delete vectors by namespace / object | `204` |
| `POST` | `/retrieve/invoke` | LangServe retrieval (semantic search) | `200 RetrieveResponse` |
| `GET` | `/retrieve/invoke` | Schema introspection (LangServe) | `200` |
| `GET` | `/health` | Readiness probe (checks TEI + Qdrant + Postgres) | `200` or `503` |

---

## Ingest Request

```json
{
  "artifact_ref": {
    "bucket": "parsed-chunks",
    "key": "<uuid4>.json"
  }
}
```

Query parameters:
- `namespace` (required): UUID — the target namespace
- `group_id` (optional): UUID — connector group for enterprise indexing
- `obj_id` (optional): UUID — if provided, used as the deduplication key instead of deriving from source
- `pipeline_type` (optional): `simple` or `semi_structured` — **indexing service only**; not yet wired into the storage upload endpoint or `IngestionTaskDetails` contract, so callers cannot select it per-upload; `DEFAULT_PIPELINE_TYPE` applies to all tasks

The service reads the `ParseArtifact` from MinIO using `artifact_ref`, embeds the chunks,
writes vectors to Qdrant, and deletes the artifact file from MinIO after successful indexing.

---

## Delete Request

```
DELETE /ingest?namespace=<uuid>&obj_id=<uuid>
```

Deletes all Qdrant points and RecordManager entries for the object. Prefix-deletes parent
pages from the `parent-pages` MinIO bucket.

```
DELETE /ingest?namespace=<uuid>
```

Clears the entire namespace from Qdrant and RecordManager (used for namespace deletion).

---

## Retrieval Request (LangServe format)

```json
{
  "input": "search query string",
  "config": {
    "configurable": {
      "namespace_id": "<uuid>",
      "k": 5,
      "return_parents": false
    }
  }
}
```

Response:
```json
{
  "output": [
    {
      "page_content": "text of chunk or page",
      "metadata": {
        "source": "document.pdf",
        "namespace_id": "...",
        "obj_id": "...",
        "parent_id": "...",
        "group_id": null
      }
    }
  ]
}
```

---

## Retrieval Modes

Controlled by `RETRIEVAL_MODE` env var:

| Mode | Strategy |
|------|---------|
| `dense` | TEI dense vector → Qdrant cosine similarity |
| `hybrid` | Dense + FastEmbed BM25 sparse → Qdrant Reciprocal Rank Fusion |
| `multi_stage` | Hybrid RRF → ColBERT MaxSim reranking |

See [Retrieval Modes](../guides/retrieval-modes.md) for full detail.

---

## Pipeline Types

Controlled by `DEFAULT_PIPELINE_TYPE` env var (overridable per request):

| Type | Chunking strategy | Multi-vector |
|------|-------------------|-------------|
| `simple` | `RecursiveCharacterSplitter` (chunk_size, chunk_overlap) | No |
| `semi_structured` | Structure-preserving; tables whole; LangChain `create_chunks` | Yes (summaries + originals) |

In `semi_structured` mode, the indexing service:
1. Generates a summary for each chunk (using a chat model)
2. Embeds the summary
3. Stores the original chunk in a PostgreSQL docstore keyed by the summary vector ID

---

## Parent-Page Retrieval

When `return_parents=true`:
- Chunks are returned as normal from Qdrant
- For each chunk, the service reads `metadata["parent_id"]` = `{namespace_id}/{obj_id}/p{page_no}`
- Fetches full page markdown from MinIO `parent-pages` bucket
- Returns pages (deduped by `parent_id`) instead of chunks

During indexing, each chunk's `parent_id` is computed and stamped in metadata. Pages are
written to `parent-pages/{namespace_id}/{obj_id}/p{page_no}` by the indexing service.

---

## Deduplication

LangChain `RecordManager` (table: `langchain_pg_embedding`) tracks which chunks have been
indexed per object:

- Key: `{namespace_id}/{obj_id}` (the `source` argument to `aadd_documents`)
- On re-index: unchanged chunks are skipped; changed or new chunks are upserted; removed
  chunks are deleted from Qdrant

This makes the indexing endpoint idempotent for the same document content.

---

## Metadata Stamped on Every Chunk

| Field | Value | Purpose |
|-------|-------|---------|
| `namespace_id` | UUID str | Mandatory Qdrant filter on every query |
| `obj_id` | UUID str | Per-object deletion |
| `group_id` | UUID str or null | Connector scoping (enterprise) |
| `source` | filename or path | Display label |
| `parent_id` | `{ns}/{obj}/p{n}` or null | Parent-page dereference key |
| `chunk_type` | `DocItemLabel` str | Table/text/header distinction |

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `QDRANT_HOST_URL` | `http://qdrant:6333` | |
| `QDRANT_COLLECTION_NAME` | `artemis` | |
| `TEI_HOST_URL` | `http://tei:80` | |
| `RETRIEVAL_MODE` | `dense` | `dense`, `hybrid`, `multi_stage` |
| `COLBERT_RERANKER_URL` | — | If set, activates mode-agnostic ColBERT reranking on `/retrieve` |
| `COLBERT_HOST_URL` | — | Required for `multi_stage` (late-interaction multi-vector, not currently used) |
| `COLBERT_MODEL_NAME` | `colbert-ir/colbertv2.0` | |
| `COLBERT_MAX_TOKENS_PER_DOC` | `511` | Max tokens sent to reranker per document |
| `RETRIEVE_CANDIDATES_MULTIPLIER` | `10` | Fetch `k × multiplier` candidates before reranking |
| `DEFAULT_PIPELINE_TYPE` | `simple` | `simple`, `semi_structured` |
| `DEFAULT_CHUNK_SIZE` | `1024` | Tokens per chunk (simple mode) |
| `DEFAULT_CHUNK_OVERLAP` | `100` | Token overlap between chunks |
| `SQL_DB_HOST` | `postgres` | RecordManager + docstore backend |
| `SQL_DB_PORT` | `5432` | |
| `SQL_DB_USER` | `postgres` | |
| `SQL_DB_PASSWORD` | `testpass` | |
| `SQL_DB_DATABASE` | `documents` | |
| `S3_ENDPOINT` | `http://minio:9000` | |
| `S3_ACCESS_KEY` | `minioadmin` | |
| `S3_SECRET_KEY` | `minioadmin` | |
| `S3_SECURE` | `false` | Set to `true` when `S3_ENDPOINT` uses HTTPS |
| `PAGE_BUCKET` | `parent-pages` | Parent-page doc store bucket |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-indexing` | |
