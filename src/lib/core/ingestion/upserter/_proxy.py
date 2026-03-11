# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Internal proxies that capture IDs assigned during lc_index/aindex calls.

LangChain's ``index``/``aindex`` helpers do not expose the content-hash IDs
they assign to each document in their ``IndexingResult`` return value.  The
two proxy classes here intercept the write calls that ``index``/``aindex``
make and record the IDs, making them available to the upserter after indexing
completes.

_IDCapturingVectorStore
    Wraps a ``VectorStore`` and captures the IDs returned by
    ``add_documents`` / ``aadd_documents``.

_IDCapturingDocumentIndex
    Wraps a ``DocumentIndex`` and captures the lc_hash IDs from each
    ``upsert`` / ``aupsert`` call via ``response.succeeded``.  ``lc_index``
    stamps ``doc.id = lc_hash`` before calling ``upsert``, so
    ``response.succeeded`` contains the actual docstore keys used.  Only
    documents that were genuinely written (not skipped as duplicates) appear
    in the captured list.  The upserter fetches those originals back from the
    docstore, summarizes them, and builds summary Documents whose
    ``metadata[id_key]`` is set to the lc_hash — ensuring
    ``MultiVectorRetriever`` can resolve the correct docstore entry at query
    time.

All other methods are delegated transparently via ``__getattr__``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, List, Optional

from langchain_core.documents import Document
from langchain_core.indexing.base import DeleteResponse, DocumentIndex, UpsertResponse
from langchain_core.vectorstores import VectorStore

__all__: list[str] = []  # private — not part of the public API


class _IDCapturingVectorStore(VectorStore):
    """Thin proxy around a VectorStore that records IDs written by lc_index.

    Usage::

        proxy = _IDCapturingVectorStore(real_vectorstore)
        result = await aindex(docs, record_manager, proxy, ...)
        written_ids = proxy.captured_ids   # IDs that actually hit the store
    """

    def __init__(self, wrapped: VectorStore) -> None:
        self._wrapped = wrapped
        self._captured_ids: List[str] = []

    # ------------------------------------------------------------------
    # ID capture — the two methods lc_index/aindex call for writes
    # ------------------------------------------------------------------

    def add_documents(self, documents: List[Document], **kwargs: Any) -> List[str]:
        ids = self._wrapped.add_documents(documents, **kwargs)
        self._captured_ids.extend(ids)
        return ids

    async def aadd_documents(
        self, documents: List[Document], **kwargs: Any
    ) -> List[str]:
        ids = await self._wrapped.aadd_documents(documents, **kwargs)
        self._captured_ids.extend(ids)
        return ids

    # ------------------------------------------------------------------
    # Abstract VectorStore methods — pure delegation
    # ------------------------------------------------------------------

    def add_texts(
        self,
        texts: Any,
        metadatas: Any = None,
        **kwargs: Any,
    ) -> List[str]:
        return self._wrapped.add_texts(texts, metadatas=metadatas, **kwargs)

    def similarity_search(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[Document]:
        return self._wrapped.similarity_search(query, k=k, **kwargs)

    def delete(self, ids: List[str] | None = None, **kwargs: Any) -> bool | None:
        return self._wrapped.delete(ids=ids, **kwargs)

    async def adelete(self, ids: List[str] | None = None, **kwargs: Any) -> bool | None:
        return await self._wrapped.adelete(ids=ids, **kwargs)

    @classmethod
    def from_texts(cls, *_args: Any, **_kwargs: Any) -> "_IDCapturingVectorStore":
        raise NotImplementedError(
            "_IDCapturingVectorStore is a proxy and cannot be constructed via from_texts"
        )

    # ------------------------------------------------------------------
    # Transparent delegation for everything else (delete, search, etc.)
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    # ------------------------------------------------------------------
    # Public result accessors
    # ------------------------------------------------------------------

    @property
    def captured_ids(self) -> List[str]:
        """IDs of all documents written to the store since construction.
        Use this for debug and testing ONLY, and don't call
        several index/aindex before draining the buffer"""
        return list(self._captured_ids)

    def drain(self) -> List[str]:
        """Return all accumulated IDs and clear the internal buffer.

        Use this instead of :attr:`captured_ids` when you want to avoid
        growing the buffer unboundedly across many ``add_documents`` batches.
        Safe to call multiple times; each call returns only the IDs captured
        since the previous drain.
        """
        ids = list(self._captured_ids)
        self._captured_ids.clear()
        return ids


class _IDCapturingDocumentIndex(DocumentIndex):
    """Thin proxy around a DocumentIndex that records lc_hash IDs written by
    lc_index / lc_aindex.

    ``lc_index`` stamps ``doc.id = lc_hash`` on each document before calling
    ``upsert(docs)``.  ``StoreDocumentIndex.upsert`` then reads ``doc.id`` as
    the storage key and returns it in ``UpsertResponse.succeeded``.  This
    proxy extends ``_captured_ids`` with ``response.succeeded`` so the upserter
    knows exactly which docstore keys were written in this run.

    Only documents that were genuinely written (not skipped as duplicates by
    lc_index's deduplication logic) appear in the captured list.  The upserter
    fetches those originals back from the docstore, summarizes them, and
    embeds the summaries with ``metadata[id_key] = lc_hash`` — ensuring
    ``MultiVectorRetriever`` can resolve the correct docstore entry.

    Usage::

        proxy = _IDCapturingDocumentIndex(document_index)
        await lc_aindex(originals, docstore_rm, proxy, ...)
        written_ids = proxy.drain()   # [lc_hash, ...]
        fetched = await document_index.aget(written_ids)
        # build summaries with metadata["doc_id"] = lc_hash
    """

    def __init__(self, wrapped: DocumentIndex) -> None:
        self._wrapped = wrapped
        self._captured_ids: List[str] = []

    # ------------------------------------------------------------------
    # ID capture — intercept upsert / aupsert
    # ------------------------------------------------------------------

    def upsert(self, items: Sequence[Document], /, **kwargs: Any) -> UpsertResponse:
        response = self._wrapped.upsert(items, **kwargs)
        self._captured_ids.extend(response["succeeded"])
        return response

    async def aupsert(
        self, items: Sequence[Document], /, **kwargs: Any
    ) -> UpsertResponse:
        response = await self._wrapped.aupsert(items, **kwargs)
        self._captured_ids.extend(response["succeeded"])
        return response

    # ------------------------------------------------------------------
    # Pure delegation for everything else
    # ------------------------------------------------------------------

    def get(self, ids: Sequence[str], /, **kwargs: Any) -> List[Optional[Document]]:
        return self._wrapped.get(ids, **kwargs)

    async def aget(
        self, ids: Sequence[str], /, **kwargs: Any
    ) -> List[Optional[Document]]:
        return await self._wrapped.aget(ids, **kwargs)

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> DeleteResponse:
        return self._wrapped.delete(ids, **kwargs)

    async def adelete(
        self, ids: Optional[List[str]] = None, **kwargs: Any
    ) -> DeleteResponse:
        return await self._wrapped.adelete(ids, **kwargs)

    def _get_relevant_documents(self, *args: Any, **kwargs: Any) -> List[Document]:
        return self._wrapped._get_relevant_documents(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    # ------------------------------------------------------------------
    # Result accessor
    # ------------------------------------------------------------------

    def drain(self) -> List[str]:
        """Return the captured lc_hash IDs and clear the internal buffer.

        Only IDs of documents that were actually written (not skipped as
        duplicates) are returned.  Safe to call multiple times; each call
        returns only the IDs captured since the previous drain.
        """
        ids = list(self._captured_ids)
        self._captured_ids.clear()
        return ids
