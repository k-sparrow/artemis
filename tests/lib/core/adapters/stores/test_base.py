# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for StoreDocumentIndex backed by InMemoryByteStore.

InMemoryByteStore has no I/O, so both sync and async paths are covered here.
"""

from __future__ import annotations

import pytest
from langchain_core.documents import Document
from langchain_core.stores import InMemoryByteStore

from src.lib.core.adapters.stores.base.store import StoreDocumentIndex


@pytest.fixture
def store() -> StoreDocumentIndex:
    return StoreDocumentIndex(store=InMemoryByteStore())


class TestStoreDocumentIndex:
    # ------------------------------------------------------------------
    # Sync — upsert
    # ------------------------------------------------------------------

    def test_upsert_explicit_ids(self, store: StoreDocumentIndex) -> None:
        result = store.upsert(
            [Document(page_content="hello", metadata={})], ids=["abc"]
        )
        assert result["succeeded"] == ["abc"]
        assert result["failed"] == []

    def test_upsert_generates_ids_when_absent(self, store: StoreDocumentIndex) -> None:
        result = store.upsert([Document(page_content="hello", metadata={})])
        assert len(result["succeeded"]) == 1
        assert isinstance(result["succeeded"][0], str)

    def test_upsert_overwrites_existing(self, store: StoreDocumentIndex) -> None:
        store.upsert([Document(page_content="v1", metadata={})], ids=["id-1"])
        store.upsert([Document(page_content="v2", metadata={})], ids=["id-1"])
        assert store.get(["id-1"])[0].page_content == "v2"

    def test_upsert_multiple_documents(self, store: StoreDocumentIndex) -> None:
        docs = [
            Document(page_content="doc A", metadata={}),
            Document(page_content="doc B", metadata={}),
        ]
        result = store.upsert(docs, ids=["A", "B"])
        assert set(result["succeeded"]) == {"A", "B"}

    # ------------------------------------------------------------------
    # Sync — get
    # ------------------------------------------------------------------

    def test_get_returns_upserted_document(self, store: StoreDocumentIndex) -> None:
        doc = Document(page_content="content", metadata={"source": "test.pdf"})
        store.upsert([doc], ids=["id-1"])
        retrieved = store.get(["id-1"])
        assert len(retrieved) == 1
        assert retrieved[0].page_content == "content"
        assert retrieved[0].metadata["source"] == "test.pdf"

    def test_get_unknown_id_returns_none(self, store: StoreDocumentIndex) -> None:
        assert store.get(["nonexistent"]) == [None]

    def test_get_partial_miss(self, store: StoreDocumentIndex) -> None:
        store.upsert([Document(page_content="exists", metadata={})], ids=["real"])
        result = store.get(["real", "missing"])
        assert result[0].page_content == "exists"
        assert result[1] is None

    # ------------------------------------------------------------------
    # Sync — delete
    # ------------------------------------------------------------------

    def test_delete_by_ids(self, store: StoreDocumentIndex) -> None:
        store.upsert([Document(page_content="to delete", metadata={})], ids=["del-1"])
        store.delete(["del-1"])
        assert store.get(["del-1"]) == [None]

    def test_delete_all_when_no_ids_given(self, store: StoreDocumentIndex) -> None:
        store.upsert(
            [
                Document(page_content="a", metadata={}),
                Document(page_content="b", metadata={}),
            ],
            ids=["x", "y"],
        )
        result = store.delete()
        assert result["num_deleted"] == 2
        assert store.get(["x", "y"]) == [None, None]

    def test_delete_returns_succeeded_ids(self, store: StoreDocumentIndex) -> None:
        store.upsert([Document(page_content="x", metadata={})], ids=["id-1"])
        result = store.delete(["id-1"])
        assert result["succeeded"] == ["id-1"]
        assert result["failed"] == []

    # ------------------------------------------------------------------
    # Async — upsert / get
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_aupsert_and_aget(self, store: StoreDocumentIndex) -> None:
        result = await store.aupsert(
            [Document(page_content="async content", metadata={})], ids=["async-1"]
        )
        assert result["succeeded"] == ["async-1"]
        retrieved = await store.aget(["async-1"])
        assert retrieved[0].page_content == "async content"

    @pytest.mark.asyncio
    async def test_aget_unknown_id_returns_none(
        self, store: StoreDocumentIndex
    ) -> None:
        assert await store.aget(["nonexistent"]) == [None]

    # ------------------------------------------------------------------
    # Async — delete
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_adelete_by_ids(self, store: StoreDocumentIndex) -> None:
        await store.aupsert(
            [Document(page_content="bye", metadata={})], ids=["async-del"]
        )
        await store.adelete(["async-del"])
        assert await store.aget(["async-del"]) == [None]

    @pytest.mark.asyncio
    async def test_adelete_all(self, store: StoreDocumentIndex) -> None:
        docs = [Document(page_content=f"doc {i}", metadata={}) for i in range(3)]
        await store.aupsert(docs, ids=["a", "b", "c"])
        result = await store.adelete()
        assert result["num_deleted"] == 3
        assert await store.aget(["a", "b", "c"]) == [None, None, None]
