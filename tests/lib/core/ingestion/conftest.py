# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Pytest fixtures for ingestion library tests."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import List, Iterator

import pytest
from langchain.indexes import SQLRecordManager
from langchain_core.documents import Document
from qdrant_client import AsyncQdrantClient, QdrantClient
from qdrant_client.models import Distance, VectorParams
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer
from testcontainers.qdrant import QdrantContainer

from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings
from src.lib.core.adapters.stores.sql.store import SQLDocumentIndex
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore
from tests.lib.testcontainers.tei import TEIContainer

_EMBEDDING_MODEL = "sentence-transformers/msmarco-MiniLM-L-12-v3"


# ---------------------------------------------------------------------------
# Infrastructure containers (session-scoped — started once per test session)
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
def postgres_container(request: pytest.FixtureRequest) -> PostgresContainer:
    container = PostgresContainer("postgres:16-alpine", driver="asyncpg")
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# Derived session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def embeddings(tei_container: TEIContainer) -> HuggingFaceEndpointEmbeddings:
    return HuggingFaceEndpointEmbeddings(model=tei_container.get_url() + "/embed")


@pytest.fixture(scope="session")
def async_engine(
    postgres_container: PostgresContainer,
) -> Iterator[AsyncEngine]:
    engine = create_async_engine(postgres_container.get_connection_url())
    yield engine


@pytest.fixture(scope="session")
def sync_engine(
    postgres_container: PostgresContainer,
) -> Iterator[Engine]:
    url = postgres_container.get_connection_url().replace("+asyncpg", "+psycopg")
    engine = create_engine(url)
    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def qdrant_client(
    qdrant_container: QdrantContainer,
) -> AsyncQdrantClient:
    host = qdrant_container.get_container_host_ip()
    port = qdrant_container.get_exposed_port(6333)
    client = AsyncQdrantClient(host=host, port=int(port), prefer_grpc=False)
    return client


@pytest.fixture
def vectorstore(
    request: pytest.FixtureRequest,
    qdrant_container: QdrantContainer,
    qdrant_client: AsyncQdrantClient,
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
        vectors_config={"dense": VectorParams(size=vector_size, distance=Distance.COSINE)},
    )

    def _teardown() -> None:
        sync_client.delete_collection(collection_name)
        sync_client.close()

    request.addfinalizer(_teardown)

    return QdrantVectorStore(
        client=sync_client,
        async_client=qdrant_client,
        collection_name=collection_name,
        embedding=embeddings,
        validate_collection_config=False,
    )


@pytest.fixture
async def record_manager(async_engine: AsyncEngine) -> SQLRecordManager:
    """Per-test async SQLRecordManager — use with aupsert / lc_aindex."""
    namespace = f"qdrant/{uuid.uuid4().hex}"
    rm = SQLRecordManager(namespace=namespace, engine=async_engine, async_mode=True)
    await rm.acreate_schema()
    return rm


@pytest.fixture
async def docstore_record_manager(async_engine: AsyncEngine) -> SQLRecordManager:
    """
    Per-test async SQLRecordManager for docstore originals.
    Use with SemiStructuredResources.
    """
    namespace = f"qdrant/{uuid.uuid4().hex}:originals"
    rm = SQLRecordManager(namespace=namespace, engine=async_engine, async_mode=True)
    await rm.acreate_schema()
    return rm


@pytest.fixture
async def document_index(async_engine: AsyncEngine) -> SQLDocumentIndex:
    """Per-test SQLDocumentIndex backed by the shared async engine.

    Each test gets a fresh namespace so docstores are fully isolated.
    Schema creation is handled lazily by SQLDocumentIndex itself, but we
    call it explicitly here so the table exists before the first write.
    """
    namespace = f"docstore/{uuid.uuid4().hex}"
    idx = SQLDocumentIndex.from_engine(namespace=namespace, engine=async_engine)
    await idx.acreate_schema()
    return idx


@pytest.fixture
def sync_record_manager(sync_engine: Engine) -> SQLRecordManager:
    """Per-test sync SQLRecordManager — use with upsert / lc_index."""
    namespace = f"qdrant/{uuid.uuid4().hex}"
    rm = SQLRecordManager(namespace=namespace, engine=sync_engine, async_mode=False)
    rm.create_schema()
    return rm


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------


_OBJ_ID_TEST1 = "11111111-1111-1111-1111-111111111111"
_OBJ_ID_TEST2 = "22222222-2222-2222-2222-222222222222"


@pytest.fixture
def sample_documents() -> List[Document]:
    return [
        Document(
            page_content="This is the first test document about machine learning.",
            metadata={"source": "test1.pdf", "page": 1, "obj_id": _OBJ_ID_TEST1},
        ),
        Document(
            page_content="This is the second test document about natural language processing.",  # noqa: E501
            metadata={"source": "test1.pdf", "page": 2, "obj_id": _OBJ_ID_TEST1},
        ),
        Document(
            page_content="This is a table with data: | Name | Value | | A | 1 | | B | 2 |",  # noqa: E501
            metadata={
                "source": "test2.pdf",
                "type": "table",
                "page": 1,
                "obj_id": _OBJ_ID_TEST2,
            },
        ),
    ]
