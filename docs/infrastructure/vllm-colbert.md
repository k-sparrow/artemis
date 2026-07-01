# vLLM / ColBERT

**Version:** vllm/vllm-openai:v0.20.2  
**Role:** Serves ColBERT in two independent roles — as a **reranker** (always active when
configured) and as a **late-interaction multi-vector embeddings model** for `multi_stage`
mode (not currently used).

**Compose profile:** `ai-colbert`  
**Port:** 8000 (internal), 11436 (host mapping)

---

## Model

**Default model:** `jinaai/jina-colbert-v2`

- Architecture: ColBERT (late-interaction bi-encoder)
- Token embedding dimension: 128
- Language: multilingual
- License: CC BY-NC 4.0 (non-commercial)

Like TEI, the ColBERT model is **pre-baked into the Docker image** for air-gapped
production deployments. The image build in `tools/oci/images/colbert/` bakes the model
weights during the Docker build.

---

## How ColBERT Differs from Dense Embedding

Dense embedding (TEI): one fixed-size vector per document/query.
ColBERT: one 128-dim vector per **token** in the document/query.

At query time, ColBERT uses **MaxSim aggregation**:

```
For each query token:
    score = max(dot_product(q_token, d_token) for d_token in document_tokens)

final_score = sum(max_scores for all query tokens)
```

This late-interaction pattern allows fine-grained token-level relevance matching, at
the cost of higher storage (N × 128 floats per document instead of 1 × dim).

---

## vLLM Entrypoint

vLLM serves ColBERT via two endpoints from the same process:

| Endpoint | Used by |
|----------|---------|
| `POST /v2/rerank` | Reranker (Cohere-compatible API) |
| `POST /v1/pooling` | Late-interaction embeddings (`VLLMLateInteractionEmbeddings`) |

```bash
vllm serve jinaai/jina-colbert-v2 \
  --trust-remote-code \
  --dtype bfloat16
```

The ENTRYPOINT is `vllm serve` — do **not** use
`python -m vllm.entrypoints.openai.api_server`. The `CMD` in the Dockerfile passes
only the model name and flags; nothing else.

---

## Roles in Artemis

### Role 1 — Reranker (mode-agnostic, always used when configured)

When `COLBERT_RERANKER_URL` is set, the indexing service wraps the base Qdrant retriever
with `ContextualCompressionRetriever`:

```python
# utils.py — lifespan
cohere_client = cohere.ClientV2(api_key="not-needed", base_url=settings.COLBERT_RERANKER_URL)
reranker = CohereRerank(
    model=settings.COLBERT_MODEL_NAME,
    client=cohere_client,
    max_tokens_per_doc=settings.COLBERT_MAX_TOKENS_PER_DOC,
)
```

`CohereRerank` calls the Cohere-compatible `POST /v2/rerank` endpoint on vLLM.

`NamespaceRetriever` applies it on every `/retrieve` call:

```python
k_candidates = k * self.candidates_multiplier if self.reranker else k
# ...
return (
    ContextualCompressionRetriever(base_compressor=self.reranker, base_retriever=base)
    if self.reranker
    else base
)
```

The retriever fetches `k × candidates_multiplier` (default × 10) results from Qdrant
and passes them to vLLM for reranking; the compressor returns the top-k. This is
**mode-agnostic** — reranking works on top of `dense`, `hybrid`, or `multi_stage` base
retrieval.

### Role 2 — Late-interaction multi-vector (multi_stage, not currently used)

When `COLBERT_HOST_URL` is set and `RETRIEVAL_MODE=multi_stage`, the indexing service
uses `VLLMLateInteractionEmbeddings` to embed each chunk as a matrix of token vectors:

```python
colbert = VLLMLateInteractionEmbeddings(
    url=settings.COLBERT_HOST_URL,
    model=settings.COLBERT_MODEL_NAME,
)
```

This calls `POST /v1/pooling` and receives a list-of-lists of 128-dim floats — one inner
list per token. These are stored in Qdrant as multi-vectors alongside the dense and sparse
vectors. At query time, Qdrant performs MaxSim rescoring natively on the prefetched
candidate set.

**Not currently used.** Storage overhead is N × 128 floats per chunk (where N is the
token count), compared to a single 1024-dim dense vector. For typical chunk sizes this
is a 10-50× storage expansion. Deferred until a use case justifies the overhead.

---

## Collection Storage

In the `MULTI_TENANT_MULTI_STAGE` Qdrant collection schema (multi_stage mode only), each
point carries:
- Dense vector (from TEI)
- Sparse vector (BM25 from FastEmbed)
- Multi-vector (ColBERT token embeddings from vLLM)

Storage per chunk scales with token count: a 100-token chunk produces 100 × 128 = 12,800
floats plus metadata, vs 1024 floats for the dense vector alone.

In `dense` and `hybrid` modes (the current default), no multi-vector is stored. The
reranker runs on the candidates returned by Qdrant search — it never writes to Qdrant.

---

## Retrieval Flow

### Reranker path (dense or hybrid mode, `COLBERT_RERANKER_URL` set)

```
Query
  ├──► TEI dense vector  [+ FastEmbed BM25 sparse vector if hybrid]
  │
  ▼
Qdrant: top-(k × p) candidates (p = candidates_multiplier, default 10)
  │
  ▼
vLLM POST /v2/rerank — ColBERT MaxSim cross-attention
  │
  ▼
Top-k results by ColBERT rerank score
```

### Multi-stage path (multi_stage mode — NOT currently used)

```
Query
  ├──► TEI dense vector
  ├──► FastEmbed BM25 sparse vector
  ├──► vLLM POST /v1/pooling — ColBERT token vectors
  │
  ▼
Qdrant prefetch: sparse + dense, RRF fusion (top-k × oversampling factor)
  │
  ▼
Qdrant rescore: ColBERT MaxSim on prefetched candidate set (stored multi-vectors)
  │
  ▼
Top-k results by ColBERT MaxSim score
```

The key distinction: the reranker path sends candidate **text** to vLLM at query time;
the multi-stage path uses **pre-stored token matrices** for Qdrant-native rescoring.

---

## Config

| Env var | Role | Notes |
|---------|------|-------|
| `COLBERT_RERANKER_URL` | Reranker (Role 1) | If set, activates `CohereRerank` on every `/retrieve` call; e.g. `http://colbert:8000` |
| `COLBERT_HOST_URL` | Late-interaction embeddings (Role 2) | Required when `RETRIEVAL_MODE=multi_stage`; e.g. `http://colbert:8000` |
| `COLBERT_MODEL_NAME` | Both | Default: `jinaai/jina-colbert-v2` |
| `COLBERT_MAX_TOKENS_PER_DOC` | Reranker (Role 1) | Default: `511` (max 512 for jina-colbert-v2) |
| `RETRIEVAL_MODE` | Late-interaction embeddings (Role 2) | Must be `multi_stage`; default is `dense` |

vLLM is only started under the `ai-colbert` compose profile. In `dense` or `hybrid`
modes with no `COLBERT_RERANKER_URL`, the indexing service does not contact vLLM.

---

## License Note

`jinaai/jina-colbert-v2` is released under CC BY-NC 4.0 (non-commercial use only).
For commercial production, a compatible ColBERT model under a permissive license must be
substituted. Replace `COLBERT_MODEL_NAME` and rebuild the colbert Docker image.
