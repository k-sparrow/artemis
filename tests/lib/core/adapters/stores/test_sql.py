# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for SQLDocumentIndex backed by a Postgres testcontainer.

All tests use async paths since the store is configured with an asyncpg
AsyncEngine.  A fresh namespace UUID is assigned per test for isolation;
no explicit teardown is required as the container is discarded after the
session.
"""

from __future__ import annotations

import uuid

import pytest
from langchain_core.documents import Document
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer

from src.lib.core.adapters.stores.sql.store import SQLDocumentIndex


# ---------------------------------------------------------------------------
# Session-scoped container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container(request: pytest.FixtureRequest) -> PostgresContainer:
    container = PostgresContainer("postgres:16-alpine", driver="asyncpg")
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# Per-test index with a unique namespace
# ---------------------------------------------------------------------------


@pytest.fixture
def document_index(postgres_container: PostgresContainer) -> SQLDocumentIndex:
    """Fresh namespace per test — prevents cross-test contamination."""
    namespace = uuid.uuid4().hex
    engine = create_async_engine(postgres_container.get_connection_url())
    return SQLDocumentIndex.from_engine(namespace=namespace, engine=engine)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSQLDocumentIndex:
    @pytest.mark.asyncio
    async def test_schema_created_automatically(
        self, document_index: SQLDocumentIndex
    ) -> None:
        """First aupsert must create the SQL schema without an explicit call."""
        result = await document_index.aupsert(
            [Document(page_content="auto schema", metadata={})], ids=["auto-1"]
        )
        assert result["succeeded"] == ["auto-1"]

    @pytest.mark.asyncio
    async def test_aupsert_and_aget(self, document_index: SQLDocumentIndex) -> None:
        doc = Document(page_content="SQL content", metadata={"source": "test.pdf"})
        result = await document_index.aupsert([doc], ids=["sql-1"])
        assert result["succeeded"] == ["sql-1"]

        retrieved = await document_index.aget(["sql-1"])
        assert retrieved[0].page_content == "SQL content"
        assert retrieved[0].metadata["source"] == "test.pdf"

    @pytest.mark.asyncio
    async def test_aupsert_multiple_documents(
        self, document_index: SQLDocumentIndex
    ) -> None:
        docs = [
            Document(page_content="doc A", metadata={}),
            Document(page_content="doc B", metadata={}),
        ]
        result = await document_index.aupsert(docs, ids=["A", "B"])
        assert set(result["succeeded"]) == {"A", "B"}

        retrieved = await document_index.aget(["A", "B"])
        assert {d.page_content for d in retrieved} == {"doc A", "doc B"}

    @pytest.mark.asyncio
    async def test_aupsert_overwrites_existing(
        self, document_index: SQLDocumentIndex
    ) -> None:
        await document_index.aupsert(
            [Document(page_content="v1", metadata={})], ids=["id-1"]
        )
        await document_index.aupsert(
            [Document(page_content="v2", metadata={})], ids=["id-1"]
        )
        retrieved = await document_index.aget(["id-1"])
        assert retrieved[0].page_content == "v2"

    @pytest.mark.asyncio
    async def test_aget_unknown_id_returns_none(
        self, document_index: SQLDocumentIndex
    ) -> None:
        result = await document_index.aget(["nonexistent"])
        assert result == [None]

    @pytest.mark.asyncio
    async def test_aget_partial_miss(self, document_index: SQLDocumentIndex) -> None:
        await document_index.aupsert(
            [Document(page_content="exists", metadata={})], ids=["real"]
        )
        result = await document_index.aget(["real", "missing"])
        assert result[0].page_content == "exists"
        assert result[1] is None

    @pytest.mark.asyncio
    async def test_adelete_by_ids(self, document_index: SQLDocumentIndex) -> None:
        await document_index.aupsert(
            [Document(page_content="to delete", metadata={})], ids=["del-1"]
        )
        result = await document_index.adelete(["del-1"])
        assert result["succeeded"] == ["del-1"]
        assert await document_index.aget(["del-1"]) == [None]

    @pytest.mark.asyncio
    async def test_adelete_all_when_no_ids_given(
        self, document_index: SQLDocumentIndex
    ) -> None:
        docs = [Document(page_content=f"doc {i}", metadata={}) for i in range(3)]
        await document_index.aupsert(docs, ids=["a", "b", "c"])
        result = await document_index.adelete()
        assert result["num_deleted"] == 3
        assert await document_index.aget(["a", "b", "c"]) == [None, None, None]

    @pytest.mark.asyncio
    async def test_acreate_schema_is_idempotent(
        self, document_index: SQLDocumentIndex
    ) -> None:
        """Calling acreate_schema twice must not raise."""
        await document_index.acreate_schema()
        await document_index.acreate_schema()
