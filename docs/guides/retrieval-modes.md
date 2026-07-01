# Retrieval Modes

Artemis supports four retrieval modes and two pipeline types that compose orthogonally.
All modes are available through the same `/retrieve/invoke` endpoint — only the indexing
service configuration changes.

---

## Choosing a Mode

| Mode | When to use |
|------|-------------|
| Dense | Default. Works well for semantic queries on well-written prose. Fastest. |
| Hybrid | Better recall on keyword-heavy queries (names, codes, jargon). No extra latency for embedding — BM25 runs server-side in Qdrant. Slight indexing overhead. |
| Multi-stage | Highest precision. Adds ColBERT reranking on top of hybrid. Use when recall matters more than latency (~2–3× slower due to ColBERT MaxSim). Requires GPU. |
| Parent-page | Orthogonal to the above. Returns full page context instead of chunks. Use when downstream LLMs need richer context than a single 200-word chunk. |

---

## Dense Mode (default)

`RETRIEVAL_MODE=dense`

```
Query
  │
  ▼
TEI (embed query → dense vector)
  │
  ▼
Qdrant cosine similarity search
  (filter: namespace_id = ?, k = N)
  │
  ▼
Top-k chunks ranked by cosine similarity
```

**Collection schema:** `MULTI_TENANT_DENSE`
- HNSW index with `m=0` (global graph disabled), `payload_m=16` (per-namespace graph)
- Cosine distance

**Use when:** General-purpose semantic search, well-written English prose, corpus under
100k chunks per namespace.

---

## Hybrid Mode

`RETRIEVAL_MODE=hybrid`

```
Query
  │
  ├──► TEI (dense vector)
  │
  └──► FastEmbed BM25 sparse encoding (in-process, no GPU)
  │
  ▼
Qdrant hybrid search
  (Reciprocal Rank Fusion of dense + sparse results)
  │
  ▼
Top-k chunks fused by RRF score
```

**Collection schema:** `MULTI_TENANT_HYBRID`
- Extends `MULTI_TENANT_DENSE` with a named sparse vector field
- Qdrant `SparseVectorParams(modifier=Modifier.IDF)` — server-side BM25 with global IDF
- FastEmbed `Qdrant/bm25` model runs in-process in the indexing service

**Sparse model baking:** The FastEmbed BM25 model is pre-baked into the indexing service
Docker image (air-gapped deployments cannot download at runtime).

**Use when:** Documents contain technical terms, product names, codes, or jargon where
exact keyword matching improves recall over pure dense search.

---

## Multi-Stage Mode

`RETRIEVAL_MODE=multi_stage`

```
Query
  │
  ├──► TEI (dense vector)
  ├──► FastEmbed BM25 sparse encoding
  │
  ▼
Qdrant prefetch: sparse + dense (RRF fusion)
  │
  ▼
ColBERT MaxSim reranking
  (VLLMLateInteractionEmbeddings → /pooling on vLLM)
  │
  ▼
Top-k chunks reranked by ColBERT MaxSim score
```

**Collection schema:** `MULTI_TENANT_MULTI_STAGE`
- Extends `MULTI_TENANT_HYBRID` with a ColBERT multi-vector field
- ColBERT: 128-dim vectors, MAX_SIM aggregation
- Late-interaction: per-token embeddings allow fine-grained token-level matching

**Requirements:** vLLM serving `jinaai/jina-colbert-v2` (or compatible ColBERT model),
GPU recommended.

**Config:**
```
COLBERT_HOST_URL      http://colbert:8000
COLBERT_MODEL_NAME    jinaai/jina-colbert-v2
```

**Storage overhead:** Late interaction models store an N×128 token-level matrix per
chunk, where N is the number of tokens in the chunk. Per-chunk storage grows linearly
with token count — significantly larger than a single dense vector (768 floats) or a
sparse BM25 vector. At scale this makes `multi_stage` impractical; it is **not
currently used** for this reason.

**Use when:** Precision matters more than latency. Legal, medical, or technical corpora
where subtle semantic differences determine relevance.

---

## Parent-Page Retrieval

Available as a flag on any retrieval mode. Controlled by `return_parents` in the
retrieval request config (or as the indexing service default).

```
Query
  │
  ▼
[Any retrieval mode → chunks]
  │
  ▼ (if return_parents=true)
For each chunk: read parent_id from metadata
  parent_id = "{namespace_id}/{obj_id}/p{page_no}"
  │
  ▼
MinIO doc store (parent-pages bucket)
  Read full page markdown for each parent_id
  │
  ▼
Return pages instead of (or alongside) chunks
```

**How parent_id is assigned:** The parsing service extracts page-delimited markdown from
`DoclingDocument`. The indexing service writes each page to the `parent-pages` MinIO bucket
at key `{namespace_id}/{obj_id}/p{page_no}`. Chunks are embedded with
`metadata["parent_id"] = "{namespace_id}/{obj_id}/p{page_no}"`.

**Deletion:** `DELETE /ingest?namespace=<ns>&obj_id=<obj>` prefix-deletes all pages for
the object from MinIO alongside the Qdrant point deletion.

**Use when:** The LLM needs broader context than a single chunk to answer the question.
Particularly useful for tables and section headers that are short as chunks but need
surrounding paragraphs to be interpretable.

---

## Pipeline Types

Pipeline type is orthogonal to retrieval mode — it controls how documents are chunked
and indexed, not how queries are executed.

| Pipeline type | Chunking | Retrieval behaviour |
|---------------|----------|---------------------|
| `SIMPLE` | RecursiveCharacterSplitter (default: 1000/200) | Chunk-level retrieval for all modes |
| `SEMI_STRUCTURED` | Structure-preserving; tables kept whole | MultiVectorRetriever: stores summaries in vectorstore, originals in PostgreSQL docstore |

**SEMI_STRUCTURED + parent-page:** Both layers are active. Queries first find summary vectors,
dereference to original chunks, then optionally dereference to parent pages.

**Setting the pipeline type:**
```
DEFAULT_PIPELINE_TYPE=simple        # or semi_structured
CHUNK_SIZE=1000
CHUNK_OVERLAP=200
```

Pipeline type can also be set per-request in the `/ingest` call (not per-query).

---

## Retrieval Request Format

The indexing service exposes a LangServe-compatible endpoint:

```bash
POST /retrieve/invoke
Content-Type: application/json

{
  "input": "your search query",
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
      "page_content": "chunk or page text",
      "metadata": {
        "source": "report.pdf",
        "namespace_id": "...",
        "obj_id": "...",
        "parent_id": "...",
        "group_id": null
      }
    }
  ]
}
```

The MCP server's `retrieve` tool wraps this endpoint and is the primary interface for AI
clients — see [MCP Server](../services/mcp.md).
