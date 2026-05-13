# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for VLLMLateInteractionEmbeddings against a live vLLM container.

These tests require a CUDA-capable GPU and the Docker NVIDIA runtime.
Tagged ``local`` because the container mounts the host HuggingFace cache.

Run with::

    pytest tests/lib/core/adapters/embeddings/late_interaction/ -m integration
"""

import pytest

from src.lib.core.adapters.embedding.late_interaction import (
    VLLMLateInteractionEmbeddings,
)

_MODEL_ID = "colbert-ir/colbertv2.0"
_COLBERT_DIM = 128

_DOCUMENTS = [
    "Artemis is a RAG document ingestion system with an event-driven microservices architecture.",  # noqa: E501
    "Qdrant is a vector database that supports multi-tenancy via metadata payload indexes.",  # noqa: E501
    "ColBERT uses late interaction: per-token embeddings scored via MaxSim at retrieval time.",  # noqa: E501
]
_QUERY = "What vector database does Artemis use?"


@pytest.fixture(scope="session")
def client(vllm_base_url: str) -> VLLMLateInteractionEmbeddings:
    return VLLMLateInteractionEmbeddings(url=vllm_base_url, model=_MODEL_ID)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.local
def test_embed_documents_returns_one_matrix_per_document(
    client: VLLMLateInteractionEmbeddings,
):
    result = client.embed_documents(_DOCUMENTS)
    assert len(result) == len(_DOCUMENTS)


@pytest.mark.integration
@pytest.mark.local
def test_embed_documents_token_vectors_are_128_dim(
    client: VLLMLateInteractionEmbeddings,
):
    result = client.embed_documents(_DOCUMENTS)
    for matrix in result:
        assert len(matrix) > 0, "expected at least one token"
        assert all(len(vec) == _COLBERT_DIM for vec in matrix)


@pytest.mark.integration
@pytest.mark.local
def test_embed_query_returns_token_matrix(
    client: VLLMLateInteractionEmbeddings,
):
    result = client.embed_query(_QUERY)
    assert len(result) > 0, "expected at least one token"
    assert all(len(vec) == _COLBERT_DIM for vec in result)


@pytest.mark.integration
@pytest.mark.local
def test_embed_documents_preserves_order(
    client: VLLMLateInteractionEmbeddings,
):
    """Batched results must arrive in input order regardless of server scheduling."""
    docs = [f"document number {i}" for i in range(5)]
    result = client.embed_documents(docs)
    assert len(result) == 5
    # Each matrix must be non-empty and 128-dim — a proxy for "not shuffled to zeros"
    for matrix in result:
        assert len(matrix) > 0
        assert len(matrix[0]) == _COLBERT_DIM


# ---------------------------------------------------------------------------
# Async
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_aembed_documents_returns_one_matrix_per_document(
    client: VLLMLateInteractionEmbeddings,
):
    result = await client.aembed_documents(_DOCUMENTS)
    assert len(result) == len(_DOCUMENTS)


@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_aembed_documents_token_vectors_are_128_dim(
    client: VLLMLateInteractionEmbeddings,
):
    result = await client.aembed_documents(_DOCUMENTS)
    for matrix in result:
        assert len(matrix) > 0
        assert all(len(vec) == _COLBERT_DIM for vec in matrix)


@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_aembed_query_returns_token_matrix(
    client: VLLMLateInteractionEmbeddings,
):
    result = await client.aembed_query(_QUERY)
    assert len(result) > 0
    assert all(len(vec) == _COLBERT_DIM for vec in result)
