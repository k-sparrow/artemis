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

_COLBERT_DIM = 128

_DOCUMENTS = [
    "Artemis is a RAG document ingestion system with an event-driven microservices architecture.",  # noqa: E501
    "Qdrant is a vector database that supports multi-tenancy via metadata payload indexes.",  # noqa: E501
    "ColBERT uses late interaction: per-token embeddings scored via MaxSim at retrieval time.",  # noqa: E501
]
_QUERY = "What vector database does Artemis use?"


@pytest.fixture(scope="session")
def client(vllm_base_url: str, vllm_model_id: str) -> VLLMLateInteractionEmbeddings:
    return VLLMLateInteractionEmbeddings(url=vllm_base_url, model=vllm_model_id)


@pytest.mark.integration
@pytest.mark.local
class TestEmbedDocuments:
    def test_returns_one_matrix_per_document(
        self, client: VLLMLateInteractionEmbeddings
    ):
        result = client.embed_documents(_DOCUMENTS)
        assert len(result) == len(_DOCUMENTS)

    def test_token_vectors_are_128_dim(self, client: VLLMLateInteractionEmbeddings):
        result = client.embed_documents(_DOCUMENTS)
        for matrix in result:
            assert len(matrix) > 0, "expected at least one token"
            assert all(len(vec) == _COLBERT_DIM for vec in matrix)

    def test_preserves_order(self, client: VLLMLateInteractionEmbeddings):
        """
        Index-sorted response must match input order regardless of server scheduling.
        """
        docs = [f"document number {i}" for i in range(5)]
        result = client.embed_documents(docs)
        assert len(result) == 5
        for matrix in result:
            assert len(matrix) > 0
            assert len(matrix[0]) == _COLBERT_DIM

    @pytest.mark.asyncio
    async def test_async_returns_one_matrix_per_document(
        self, client: VLLMLateInteractionEmbeddings
    ):
        result = await client.aembed_documents(_DOCUMENTS)
        assert len(result) == len(_DOCUMENTS)

    @pytest.mark.asyncio
    async def test_async_token_vectors_are_128_dim(
        self, client: VLLMLateInteractionEmbeddings
    ):
        result = await client.aembed_documents(_DOCUMENTS)
        for matrix in result:
            assert len(matrix) > 0
            assert all(len(vec) == _COLBERT_DIM for vec in matrix)


@pytest.mark.integration
@pytest.mark.local
class TestEmbedQuery:
    def test_returns_token_matrix(self, client: VLLMLateInteractionEmbeddings):
        result = client.embed_query(_QUERY)
        assert len(result) > 0, "expected at least one token"
        assert all(len(vec) == _COLBERT_DIM for vec in result)

    @pytest.mark.asyncio
    async def test_async_returns_token_matrix(
        self, client: VLLMLateInteractionEmbeddings
    ):
        result = await client.aembed_query(_QUERY)
        assert len(result) > 0
        assert all(len(vec) == _COLBERT_DIM for vec in result)
