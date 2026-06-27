# TEI — Text Embeddings Inference

**Version:** ghcr.io/huggingface/text-embeddings-inference:1.6  
**Role:** Dense text embedding inference. Converts text chunks and queries into fixed-size
vectors that are stored in Qdrant and compared at query time.

**Compose profile:** `ai-tei`  
**Port:** 80 (internal), 11435 (host mapping)

---

## Model

**Default model:** `Alibaba-NLP/gte-large-en-v1.5`

- Architecture: encoder-only transformer (BERT-style)
- Embedding dimension: 1024
- Max tokens: 8192
- Language: English
- License: Apache 2.0

The model is **pre-baked into the Docker image** for air-gapped production deployments
(no internet download at runtime). The image build in `tools/oci/images/tei/` bakes the
model weights during the Docker build.

To use a different embedding model, rebuild the TEI image with the new model name and
update `TEI_HOST_URL` to point to the new instance.

---

## Integration

The indexing service connects to TEI via `TEI_HOST_URL`:

```
TEI_HOST_URL=http://tei:80   # default in dev compose
```

LangChain's `HuggingFaceEndpointEmbeddings` adapter is used:

```python
embeddings = HuggingFaceEndpointEmbeddings(model=settings.TEI_HOST_URL)
```

This adapter calls TEI's `/embed` endpoint with batches of text strings and receives
numpy float32 arrays.

---

## Health Check

The indexing service's readiness probe checks TEI health before accepting traffic:

```bash
# TEI readiness (from inside Docker network)
curl http://tei:80/health

# From host
curl http://localhost:11435/health
```

---

## GPU vs CPU

TEI can run on CPU (slow) or GPU (fast). The compose profile `ai-tei` does not enforce
GPU allocation — add `deploy.resources.reservations.devices` to the compose service if
GPU is required:

```yaml
deploy:
  resources:
    reservations:
      devices:
        - driver: nvidia
          count: 1
          capabilities: [gpu]
```

For development without GPU, TEI on CPU is acceptable for small corpora
(indexing throughput is lower but functional).

---

## Relationship to ColBERT

TEI handles **dense** embeddings only. ColBERT late-interaction embeddings are served by
vLLM — see [vLLM / ColBERT](vllm-colbert.md). In `multi_stage` mode, both TEI and vLLM
are required.

In `dense` and `hybrid` modes, only TEI is needed. ColBERT/vLLM is not started.
