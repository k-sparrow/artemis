# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Fixtures for QdrantVectorStore retrieval-mode integration tests.

Session-scoped Qdrant, TEI, and vLLM containers are started once per test
session. Each test gets a fresh collection (created and deleted per-function)
so runs are fully isolated.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from qdrant_client import AsyncQdrantClient, QdrantClient, models
from testcontainers.qdrant import QdrantContainer

from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings
from src.lib.core.adapters.embedding.late_interaction import (
    VLLMLateInteractionEmbeddings,
)
from src.lib.core.adapters.embedding.sparse import FastEmbedSparseEmbeddings
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore, RetrievalMode
from tests.lib.testcontainers.tei import TEIContainer
from tests.lib.testcontainers.vllm import VLLMContainer

_EMBEDDING_MODEL = "sentence-transformers/msmarco-MiniLM-L-12-v3"
_COLBERT_MODEL = "colbert-ir/colbertv2.0"
_COLBERT_DIM = 128


# ---------------------------------------------------------------------------
# Session-scoped infrastructure
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def qdrant_container(request: pytest.FixtureRequest) -> QdrantContainer:
    container = QdrantContainer("qdrant/qdrant:v1.17")
    container.start()
    request.addfinalizer(container.stop)
    return container


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
def vllm_container(request: pytest.FixtureRequest) -> VLLMContainer:
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = VLLMContainer(model_id=_COLBERT_MODEL)
    if hf_cache.exists():
        container.with_hf_cache(str(hf_cache))
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def embeddings(tei_container: TEIContainer) -> HuggingFaceEndpointEmbeddings:
    return HuggingFaceEndpointEmbeddings(model=tei_container.get_url() + "/embed")


@pytest.fixture(scope="session")
def late_interaction_embeddings(
    vllm_container: VLLMContainer,
) -> VLLMLateInteractionEmbeddings:
    return VLLMLateInteractionEmbeddings(
        url=vllm_container.get_url(),
        model=vllm_container.model_id,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _qdrant_clients(
    container: QdrantContainer,
) -> tuple[QdrantClient, AsyncQdrantClient]:
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(6333))
    return (
        QdrantClient(host=host, port=port, prefer_grpc=False),
        AsyncQdrantClient(host=host, port=port, prefer_grpc=False),
    )


# ---------------------------------------------------------------------------
# Per-test collection fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dense_vectorstore(
    request: pytest.FixtureRequest,
    qdrant_container: QdrantContainer,
    embeddings: HuggingFaceEndpointEmbeddings,
) -> QdrantVectorStore:
    """Fresh Qdrant dense-only collection per test — deleted on teardown."""
    collection_name = uuid.uuid4().hex
    sync_client, async_client = _qdrant_clients(qdrant_container)

    vector_size = len(embeddings.embed_query("dummy"))
    sync_client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=vector_size,
            distance=models.Distance.COSINE,
        ),
        hnsw_config=models.HnswConfigDiff(payload_m=16, m=0),
    )

    request.addfinalizer(lambda: sync_client.delete_collection(collection_name))

    return QdrantVectorStore(
        client=sync_client,
        async_client=async_client,
        collection_name=collection_name,
        embedding=embeddings,
        retrieval_mode=RetrievalMode.DENSE,
        validate_collection_config=False,
    )


@pytest.fixture
def hybrid_vectorstore(
    request: pytest.FixtureRequest,
    qdrant_container: QdrantContainer,
    embeddings: HuggingFaceEndpointEmbeddings,
) -> QdrantVectorStore:
    """Fresh Qdrant hybrid collection per test — deleted on teardown.

    The collection has an unnamed dense vector and a BM25 sparse vector
    named ``langchain-sparse`` with server-side IDF weighting.
    """
    collection_name = uuid.uuid4().hex
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))

    sync_client = QdrantClient(host=host, port=port, prefer_grpc=False)
    async_client = AsyncQdrantClient(host=host, port=port, prefer_grpc=False)

    vector_size = len(embeddings.embed_query("dummy"))
    sync_client.create_collection(
        collection_name=collection_name,
        vectors_config={"size": vector_size, "distance": "Cosine"},
        sparse_vectors_config={
            "langchain-sparse": models.SparseVectorParams(
                modifier=models.Modifier.IDF,
                index=models.SparseIndexParams(on_disk=False),
            )
        },
    )

    def _teardown() -> None:
        sync_client.delete_collection(collection_name)
        sync_client.close()

    request.addfinalizer(_teardown)

    return QdrantVectorStore(
        client=sync_client,
        async_client=async_client,
        collection_name=collection_name,
        embedding=embeddings,
        retrieval_mode=RetrievalMode.HYBRID,
        sparse_embedding=FastEmbedSparseEmbeddings(),
        validate_collection_config=False,
    )


@pytest.fixture
def multi_stage_vectorstore(
    request: pytest.FixtureRequest,
    qdrant_container: QdrantContainer,
    embeddings: HuggingFaceEndpointEmbeddings,
    late_interaction_embeddings: VLLMLateInteractionEmbeddings,
) -> QdrantVectorStore:
    """Fresh Qdrant multi-stage collection per test — deleted on teardown.

    Three vector spaces in one collection:
    - ``dense``           — named dense vector (COSINE)
    - ``langchain-sparse``— BM25 sparse with IDF modifier
    - ``colbert``         — 128-dim multi-vector with MaxSim comparator
    """
    collection_name = uuid.uuid4().hex
    sync_client, async_client = _qdrant_clients(qdrant_container)

    vector_size = len(embeddings.embed_query("dummy"))
    sync_client.create_collection(
        collection_name=collection_name,
        vectors_config={
            "dense": models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE,
            ),
            "colbert": models.VectorParams(
                size=_COLBERT_DIM,
                distance=models.Distance.COSINE,
                multivector_config=models.MultiVectorConfig(
                    comparator=models.MultiVectorComparator.MAX_SIM,
                ),
            ),
        },
        sparse_vectors_config={
            "langchain-sparse": models.SparseVectorParams(
                modifier=models.Modifier.IDF,
                index=models.SparseIndexParams(on_disk=False),
            ),
        },
        hnsw_config=models.HnswConfigDiff(payload_m=16, m=0),
    )

    request.addfinalizer(lambda: sync_client.delete_collection(collection_name))

    return QdrantVectorStore(
        client=sync_client,
        async_client=async_client,
        collection_name=collection_name,
        embedding=embeddings,
        retrieval_mode=RetrievalMode.MULTI_STAGE,
        sparse_embedding=FastEmbedSparseEmbeddings(),
        late_interaction_embedding=late_interaction_embeddings,
        vector_name="dense",
        validate_collection_config=False,
    )


# ---------------------------------------------------------------------------
# Parameterized fixture for generic cross-mode tests
# ---------------------------------------------------------------------------


@pytest.fixture(
    params=["dense", "hybrid", "multi_stage"],
    ids=["dense", "hybrid", "multi_stage"],
)
def vectorstore(request: pytest.FixtureRequest) -> QdrantVectorStore:
    """QdrantVectorStore parameterized over all three retrieval modes.

    Delegates to the mode-specific fixture so each mode gets its own
    isolated collection with the correct vector configuration.
    """
    return request.getfixturevalue(f"{request.param}_vectorstore")
