# Qdrant

**Version:** qdrant/qdrant:v1.17-unprivileged (port 6333)  
**Role:** Vector database for dense embeddings, sparse BM25 vectors, and ColBERT
multi-vectors. All namespaces share a single collection; isolation is enforced through
payload filters.

---

## Collection

A single shared collection is used for all namespaces. The collection name is set by:
```
QDRANT_COLLECTION_NAME=artemis   # default in compose
```

The collection schema is created by the indexing service on startup
(`QdrantVectorStoreHandler.acreate()`) if it does not already exist.

---

## Collection Schema Variants

Three pre-built schemas are defined in `src/lib/core/adapters/vectorstore/config.py`:

### `MULTI_TENANT_DENSE` (default, `RETRIEVAL_MODE=dense`)

- Distance: Cosine
- HNSW config: `m=0`, `payload_m=16`
  - `m=0` disables the global HNSW graph
  - `payload_m=16` builds a per-payload-value HNSW graph (one per namespace_id value)
  - This is the standard Qdrant multi-tenant configuration: each namespace has its own
    independent graph, preventing cross-namespace vector neighbourhood contamination
- Dense vector: TEI model dimension (auto-detected from the embedding model on first use)
- Payload indexes: `namespace_id` (keyword), `obj_id` (keyword), `group_id` (keyword)

### `MULTI_TENANT_HYBRID` (`RETRIEVAL_MODE=hybrid`)

Extends `MULTI_TENANT_DENSE` with:
- Sparse vector: `SparseVectorParams(modifier=Modifier.IDF)` — server-side BM25 with
  global IDF computed over all vectors in the collection

### `MULTI_TENANT_MULTI_STAGE` (`RETRIEVAL_MODE=multi_stage`)

Extends `MULTI_TENANT_HYBRID` with:
- Multi-vector (ColBERT): 128-dim, `MaxSim` aggregation (per-token late interaction)

---

## Multi-Tenancy Enforcement

All chunks are stamped with `namespace_id` in `Document.metadata` by the indexing service:

```python
doc.metadata["namespace_id"] = str(namespace_id)
doc.metadata["obj_id"]       = str(obj_id)
doc.metadata["group_id"]     = str(group_id) if group_id else None
doc.metadata["source"]       = source
doc.metadata["parent_id"]    = parent_id    # "{namespace_id}/{obj_id}/p{page_no}"
```

LangChain's `QdrantVectorStore` translates the `namespace_id` metadata into a mandatory
Qdrant `Filter` on every search:

```python
filter=models.Filter(
    must=[
        models.FieldCondition(
            key="metadata.namespace_id",
            match=models.MatchValue(value=str(namespace_id))
        )
    ]
)
```

A query without `namespace_id` is not possible through the indexing service API — the
namespace is always passed as a required query parameter.

---

## Payload Indexes

| Field | Type | Purpose |
|-------|------|---------|
| `metadata.namespace_id` | keyword | Primary tenant partition — required on every search |
| `metadata.obj_id` | keyword | Per-object deletion without namespace-wide scan |
| `metadata.group_id` | keyword | Group-scoped queries (list/delete by connector) |

`group_id` is indexed for deletion scoping. Filtering within a namespace by `group_id` is
efficient enough without a dedicated HNSW graph (the namespace payload filter pre-scopes
to a small candidate set).

---

## Deletion

**Single object:** `DELETE /ingest?namespace=<uuid>&obj_id=<uuid>`
- Deletes all Qdrant points where `metadata.obj_id = <obj_id>` AND
  `metadata.namespace_id = <namespace_id>`
- Also removes LangChain RecordManager entries for the object

**Full namespace:** `DELETE /ingest?namespace=<uuid>` (no `obj_id`)
- Calls `pipeline.aprocess([])` — clears all vectors and record-manager entries for the namespace
- Triggered by `tasks.delete_namespace` (fired after namespace soft-delete)

**Group delete:** storage service enumerates `ingested_objects` by `group_id` and dispatches
a `tasks.ingest(upload_action=DELETE)` per object. Qdrant deletes happen per-object.

---

## Deduplication (RecordManager)

LangChain's `RecordManager` (backed by a PostgreSQL table) tracks which chunks have been
indexed for each object:

- Deduplication key: `obj_id` (passed as the `source` to RecordManager)
- On re-upload of the same file, unchanged chunks are skipped (no re-embedding)
- Changed chunks are upserted; removed chunks are cleaned up from Qdrant

The `RecordManager` table is in the same `documents` PostgreSQL database.

---

## qdrant.py Note

`src/lib/core/adapters/vectorstore/qdrant.py` is a fork of an unreleased
`langchain-qdrant` development branch. It was forked to add native async support before
upstream released it. Dropping this fork requires a `langchain >= 1.0.0` migration;
deferred until after Artemis v1.0.0.

---

## Accessing Qdrant

Port 6333 is exposed to the host in **dev mode only**. In the release compose the port
is internal — only services within the Docker network can reach it.

```bash
# REST API (dev mode, from host)
curl http://localhost:6333/collections

# List collection info
curl http://localhost:6333/collections/artemis

# Count vectors in a namespace (approximate)
curl -X POST http://localhost:6333/collections/artemis/points/count \
  -H 'Content-Type: application/json' \
  -d '{"filter": {"must": [{"key": "metadata.namespace_id", "match": {"value": "<uuid>"}}]}}'
```
