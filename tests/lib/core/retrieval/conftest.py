# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Session-scoped infrastructure fixtures for retrieval integration tests."""

from __future__ import annotations

import uuid
from pathlib import Path

import cohere
import pytest
from qdrant_client import AsyncQdrantClient, QdrantClient
from qdrant_client.models import Distance, VectorParams
from testcontainers.qdrant import QdrantContainer

from src.lib.core.adapters.document_compressors.cohere import CohereRerank
from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore
from tests.lib.testcontainers.tei import TEIContainer
from tests.lib.testcontainers.vllm import VLLMContainer


_EMBEDDING_MODEL = "sentence-transformers/msmarco-MiniLM-L-12-v3"
_COLBERT_MODEL = "colbert-ir/colbertv2.0"


# ---------------------------------------------------------------------------
# Infrastructure containers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def tei_container(request: pytest.FixtureRequest) -> TEIContainer:
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = TEIContainer(model_id=_EMBEDDING_MODEL)
    if hf_cache.exists():
        container.with_hf_cache(str(hf_cache))
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def qdrant_container(request: pytest.FixtureRequest) -> QdrantContainer:
    container = QdrantContainer("qdrant/qdrant:v1.17")
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def vllm_rerank_url(request: pytest.FixtureRequest) -> str:
    """Session-scoped vLLM container serving ColBERT for reranking (GPU required)."""
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = VLLMContainer(model_id=_COLBERT_MODEL)
    if hf_cache.exists():
        container.with_hf_cache(str(hf_cache))
    container.start()
    request.addfinalizer(container.stop)
    return container.get_url()


# ---------------------------------------------------------------------------
# Derived session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def embeddings(tei_container: TEIContainer) -> HuggingFaceEndpointEmbeddings:
    return HuggingFaceEndpointEmbeddings(model=tei_container.get_url() + "/embed")


@pytest.fixture(scope="session")
def reranker(vllm_rerank_url: str) -> CohereRerank:
    client = cohere.ClientV2(api_key="not-needed", base_url=vllm_rerank_url)
    return CohereRerank(model=_COLBERT_MODEL, top_n=2, client=client)


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def vectorstore(
    request: pytest.FixtureRequest,
    qdrant_container: QdrantContainer,
    embeddings: HuggingFaceEndpointEmbeddings,
) -> QdrantVectorStore:
    """Fresh Qdrant collection per test, deleted on teardown."""
    collection_name = uuid.uuid4().hex
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))
    vector_size = len(embeddings.embed_query("dummy"))

    sync_client = QdrantClient(host=host, port=port, prefer_grpc=False)
    sync_client.create_collection(
        collection_name=collection_name,
        vectors_config={
            "dense": VectorParams(size=vector_size, distance=Distance.COSINE)
        },
    )
    async_client = AsyncQdrantClient(host=host, port=port, prefer_grpc=False)

    def _teardown() -> None:
        sync_client.delete_collection(collection_name)
        sync_client.close()

    request.addfinalizer(_teardown)

    return QdrantVectorStore(
        client=sync_client,
        async_client=async_client,
        collection_name=collection_name,
        embedding=embeddings,
        validate_collection_config=False,
    )
