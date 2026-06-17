# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the ParentPageRetriever decorator (no containers).

Wraps a fake inner ``BaseRetriever`` (standing in for dense / hybrid / reranked
retrieval) and dereferences each returned chunk's ``parent_id`` against an
in-memory byte store, the same way ``ContextualCompressionRetriever`` composes a
reranker over search.
"""

from __future__ import annotations

from typing import List

import pytest
from langchain.storage import create_kv_docstore
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_core.stores import InMemoryByteStore

from src.lib.core.retrieval.parent_page import ParentPageRetriever


class FakeRetriever(BaseRetriever):
    """Returns a fixed chunk list — the inner search the decorator composes over."""

    docs: List[Document]

    def _get_relevant_documents(self, query, *, run_manager) -> List[Document]:
        return self.docs


def _chunk(parent_id: str | None, text: str = "chunk") -> Document:
    meta = {"parent_id": parent_id} if parent_id is not None else {}
    return Document(page_content=text, metadata=meta)


async def _store_with_pages(*keys: str) -> InMemoryByteStore:
    store = InMemoryByteStore()
    docstore = create_kv_docstore(store)
    await docstore.amset(
        [
            (k, Document(page_content=f"PAGE {k}", metadata={"page_key": k}))
            for k in keys
        ]
    )
    return store


class TestParentPageRetriever:
    @pytest.mark.asyncio
    async def test_dedups_chunks_to_parent_pages(self) -> None:
        store = await _store_with_pages("ns/obj/p1", "ns/obj/p2")
        inner = FakeRetriever(
            docs=[
                _chunk("ns/obj/p1"),
                _chunk("ns/obj/p1"),  # duplicate parent
                _chunk("ns/obj/p2"),
            ]
        )
        retriever = ParentPageRetriever(base_retriever=inner, byte_store=store)

        pages = await retriever.ainvoke("q")

        # N chunks → fewer parent pages, order-stable, no duplicates.
        assert [p.page_content for p in pages] == ["PAGE ns/obj/p1", "PAGE ns/obj/p2"]

    @pytest.mark.asyncio
    async def test_missing_page_is_dropped(self) -> None:
        store = await _store_with_pages("ns/obj/p1")
        inner = FakeRetriever(docs=[_chunk("ns/obj/p1"), _chunk("ns/obj/gone")])
        retriever = ParentPageRetriever(base_retriever=inner, byte_store=store)

        pages = await retriever.ainvoke("q")

        assert [p.page_content for p in pages] == ["PAGE ns/obj/p1"]

    @pytest.mark.asyncio
    async def test_chunk_without_parent_id_contributes_nothing(self) -> None:
        store = await _store_with_pages("ns/obj/p1")
        inner = FakeRetriever(docs=[_chunk(None), _chunk("ns/obj/p1")])
        retriever = ParentPageRetriever(base_retriever=inner, byte_store=store)

        pages = await retriever.ainvoke("q")

        assert [p.page_content for p in pages] == ["PAGE ns/obj/p1"]

    @pytest.mark.asyncio
    async def test_no_chunks_returns_empty(self) -> None:
        store = await _store_with_pages("ns/obj/p1")
        retriever = ParentPageRetriever(
            base_retriever=FakeRetriever(docs=[]), byte_store=store
        )

        assert await retriever.ainvoke("q") == []

    @pytest.mark.asyncio
    async def test_custom_id_key_is_honoured(self) -> None:
        store = await _store_with_pages("k1")
        chunk = Document(page_content="c", metadata={"my_parent": "k1"})
        retriever = ParentPageRetriever(
            base_retriever=FakeRetriever(docs=[chunk]),
            byte_store=store,
            id_key="my_parent",
        )

        pages = await retriever.ainvoke("q")

        assert [p.page_content for p in pages] == ["PAGE k1"]

    def test_sync_invoke_is_not_supported(self) -> None:
        retriever = ParentPageRetriever(
            base_retriever=FakeRetriever(docs=[]), byte_store=InMemoryByteStore()
        )
        with pytest.raises(NotImplementedError):
            retriever.invoke("q")
