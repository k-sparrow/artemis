# vLLM / ColBERT

**Version:** vllm/vllm-openai:v0.20.2  
**Role:** Late-interaction reranking for MULTI_STAGE retrieval mode. Serves ColBERT
multi-vector embeddings via vLLM's pooling endpoint.

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

vLLM serves ColBERT via its pooling (embedding) API:

```bash
vllm serve jinaai/jina-colbert-v2 \
  --trust-remote-code \
  --dtype bfloat16
```

The ENTRYPOINT is `vllm serve` — do **not** use
`python -m vllm.entrypoints.openai.api_server`. The `CMD` in the Dockerfile passes
only the model name and flags; nothing else.

The endpoint used by the indexing service is:
```
POST http://colbert:8000/v1/pooling
```

---

## Integration

The indexing service uses `VLLMLateInteractionEmbeddings`:

```python
colbert = VLLMLateInteractionEmbeddings(
    base_url=settings.COLBERT_HOST_URL,
    model_name=settings.COLBERT_MODEL_NAME,
)
```

This adapter calls the `/v1/pooling` endpoint and receives a list-of-lists of 128-dim
floats (one inner list per token). These are stored in Qdrant as multi-vectors.

---

## Collection Storage

In the `MULTI_TENANT_MULTI_STAGE` Qdrant collection schema, each point carries:
- Dense vector (from TEI)
- Sparse vector (BM25 from FastEmbed)
- Multi-vector (ColBERT token embeddings from vLLM)

Storage per chunk scales with token count: a 100-token chunk produces 100 × 128 = 12,800
floats plus metadata, vs 1024 floats for the dense vector alone.

---

## Retrieval Flow

Multi-stage retrieval uses ColBERT only for **reranking**, not for initial recall:

```
Query
  ├──► TEI dense vector
  ├──► FastEmbed BM25 sparse vector
  │
  ▼
Qdrant prefetch: sparse + dense, RRF fusion (top-k × oversampling factor)
  │
  ▼
Qdrant rescore: ColBERT MaxSim on prefetched candidate set
  │
  ▼
Top-k results by ColBERT MaxSim score
```

The prefetch step retrieves more candidates than needed (oversampling); ColBERT then
reranks and returns the final top-k. This limits ColBERT inference to a small candidate
set, bounding latency.

---

## Config

```
COLBERT_HOST_URL    http://colbert:8000   # from compose
COLBERT_MODEL_NAME  jinaai/jina-colbert-v2
RETRIEVAL_MODE      multi_stage
```

vLLM is only started under the `ai-colbert` compose profile. In `dense` or `hybrid`
modes, the indexing service does not attempt to contact `COLBERT_HOST_URL`.

---

## License Note

`jinaai/jina-colbert-v2` is released under CC BY-NC 4.0 (non-commercial use only).
For commercial production, a compatible ColBERT model under a permissive license must be
substituted. Replace `COLBERT_MODEL_NAME` and rebuild the colbert Docker image.
