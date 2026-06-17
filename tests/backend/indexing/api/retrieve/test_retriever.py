# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for NamespaceRetriever logic.

No containers or network.  Pydantic validates isinstance() on both
``retrieval_adapter`` and ``reranker``, so we use concrete test-double
subclasses rather than raw MagicMock.  ``build_qdrant_filter`` is patched
to verify that the correct RetrievalParams are forwarded.
"""

from __future__ import annotations

import uuid
from typing import Any, List, Optional, Sequence
from unittest.mock import MagicMock, patch

import pytest
from langchain.storage import create_kv_docstore
from langchain_core.callbacks import Callbacks
from langchain_core.documents import Document
from langchain_core.documents.compressor import BaseDocumentCompressor
from langchain_core.retrievers import BaseRetriever
from langchain_core.stores import InMemoryByteStore

from src.backend.indexing.api.retrieve.retriever import NamespaceRetriever
from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter
from src.lib.core.retrieval.parent_page import ParentPageRetriever


_NS = str(uuid.uuid4())
_GROUP = str(uuid.uuid4())
_DOC = str(uuid.uuid4())
_DOCS = [Document(page_content="result doc", metadata={})]


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeRetriever(BaseRetriever):
    """Minimal BaseRetriever that returns a fixed document list."""

    docs: List[Document] = []

    def _get_relevant_documents(
        self, query: str, *, run_manager: Any = None
    ) -> List[Document]:
        return self.docs


class _FakeAdapter(BaseRetrievalAdapter):
    """Concrete BaseRetrievalAdapter that records get_retriever() call args."""

    def __init__(self, docs: List[Document] = _DOCS) -> None:
        self._docs = docs
        self._retriever = _FakeRetriever(docs=docs)
        self.last_search_kwargs: dict = {}

    def get_retriever(self, search_type: Any, search_kwargs: Any) -> _FakeRetriever:
        self.last_search_kwargs = search_kwargs
        return self._retriever


class _FakeCompressor(BaseDocumentCompressor):
    """Concrete BaseDocumentCompressor that records compress_documents() calls."""

    compress_called: bool = False
    return_docs: List[Document] = []

    def compress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks: Optional[Callbacks] = None,
    ) -> Sequence[Document]:
        self.compress_called = True
        return self.return_docs or list(documents)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNamespaceRetriever:
    def test_raises_when_namespace_id_missing(self):
        nr = NamespaceRetriever(retrieval_adapter=_FakeAdapter())
        with pytest.raises(ValueError, match="namespace_id is required"):
            nr.invoke("query", config={"configurable": {}})

    def test_extracts_namespace_id_from_configurable(self):
        nr = NamespaceRetriever(retrieval_adapter=_FakeAdapter())
        result = nr.invoke("query", config={"configurable": {"namespace_id": _NS}})
        assert result == _DOCS

    def test_namespace_id_on_object_takes_precedence(self):
        adapter = _FakeAdapter()
        nr = NamespaceRetriever(namespace_id=_NS, retrieval_adapter=adapter)
        nr.invoke("query", config={"configurable": {}})
        assert adapter.last_search_kwargs  # get_retriever was called

    def test_k_unchanged_when_no_reranker(self):
        adapter = _FakeAdapter()
        nr = NamespaceRetriever(retrieval_adapter=adapter, candidates_multiplier=5)
        nr.invoke("query", config={"configurable": {"namespace_id": _NS, "k": 4}})
        assert adapter.last_search_kwargs["k"] == 4

    def test_k_candidates_multiplied_when_reranker_present(self):
        adapter = _FakeAdapter()
        reranker = _FakeCompressor(return_docs=_DOCS)
        nr = NamespaceRetriever(
            retrieval_adapter=adapter,
            reranker=reranker,
            candidates_multiplier=5,
        )
        nr.invoke("query", config={"configurable": {"namespace_id": _NS, "k": 3}})
        assert adapter.last_search_kwargs["k"] == 15  # 3 * 5

    def test_base_retriever_used_directly_when_no_reranker(self):
        adapter = _FakeAdapter()
        nr = NamespaceRetriever(retrieval_adapter=adapter)
        result = nr.invoke("query", config={"configurable": {"namespace_id": _NS}})
        # no reranker — result comes straight from the adapter's retriever
        assert result == _DOCS

    def test_wraps_with_compression_retriever_when_reranker_set(self):
        adapter = _FakeAdapter()
        reranker = _FakeCompressor(return_docs=_DOCS)
        nr = NamespaceRetriever(retrieval_adapter=adapter, reranker=reranker)
        nr.invoke("query", config={"configurable": {"namespace_id": _NS}})
        assert reranker.compress_called

    def test_group_id_doc_id_forwarded_to_filter(self):
        adapter = _FakeAdapter()
        nr = NamespaceRetriever(retrieval_adapter=adapter)

        with patch(
            "src.backend.indexing.api.retrieve.retriever.build_qdrant_filter"
        ) as mock_filter:
            mock_filter.return_value = MagicMock()
            nr.invoke(
                "query",
                config={
                    "configurable": {
                        "namespace_id": _NS,
                        "group_id": _GROUP,
                        "doc_id": _DOC,
                    }
                },
            )

        params = mock_filter.call_args.args[0]
        assert str(params.namespace_id) == _NS
        assert str(params.group_id) == _GROUP
        assert str(params.doc_id) == _DOC


# ---------------------------------------------------------------------------
# Parent-page composition (async; per-request return_parents re-base)
# ---------------------------------------------------------------------------


def _chunk(parent_id: str) -> Document:
    return Document(page_content="chunk", metadata={"parent_id": parent_id})


async def _store_with_pages(*keys: str) -> InMemoryByteStore:
    store = InMemoryByteStore()
    docstore = create_kv_docstore(store)
    await docstore.amset(
        [(k, Document(page_content=f"PAGE {k}", metadata={})) for k in keys]
    )
    return store


def _config(**extra: Any) -> dict:
    return {"configurable": {"namespace_id": _NS, **extra}}


class TestParentPageComposition:
    """The store-bound ParentPageRetriever is built in the service wiring and
    re-based per request onto the namespace-scoped base — NamespaceRetriever
    never holds the byte store."""

    @pytest.mark.asyncio
    async def test_return_parents_derefs_chunks_to_pages(self):
        store = await _store_with_pages("ns/obj/p1")
        # Two chunks → one parent page (dedup), proving the deref ran.
        adapter = _FakeAdapter(docs=[_chunk("ns/obj/p1"), _chunk("ns/obj/p1")])
        ppr = ParentPageRetriever(byte_store=store, id_key="parent_id")
        nr = NamespaceRetriever(retrieval_adapter=adapter, parent_page_retriever=ppr)

        result = await nr.ainvoke("q", config=_config(return_parents=True))

        assert [d.page_content for d in result] == ["PAGE ns/obj/p1"]

    @pytest.mark.asyncio
    async def test_without_return_parents_yields_chunks(self):
        store = await _store_with_pages("ns/obj/p1")
        chunks = [_chunk("ns/obj/p1")]
        adapter = _FakeAdapter(docs=chunks)
        ppr = ParentPageRetriever(byte_store=store, id_key="parent_id")
        nr = NamespaceRetriever(retrieval_adapter=adapter, parent_page_retriever=ppr)

        # Flag absent ⇒ no deref, the chunks pass straight through.
        result = await nr.ainvoke("q", config=_config())

        assert result == chunks

    @pytest.mark.asyncio
    async def test_return_parents_without_decorator_is_graceful(self):
        chunks = [_chunk("ns/obj/p1")]
        adapter = _FakeAdapter(docs=chunks)
        # No page store configured ⇒ parent_page_retriever is None.
        nr = NamespaceRetriever(retrieval_adapter=adapter)

        result = await nr.ainvoke("q", config=_config(return_parents=True))

        assert result == chunks
