# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Internal vectorstore proxy that captures IDs from add_documents calls.

LangChain's ``index``/``aindex`` helpers assign content-hash UUIDs to every
document internally but do not expose those IDs in their ``IndexingResult``
return value.  This proxy intercepts the ``add_documents`` / ``aadd_documents``
calls that ``index``/``aindex`` make on the vectorstore and records the IDs
returned by the real store, making them available after indexing completes.

All other VectorStore methods are delegated transparently via ``__getattr__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from langchain_core.documents import Document
from langchain_core.vectorstores import VectorStore

if TYPE_CHECKING:
    pass

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
    def from_texts(cls, *_args: Any, **_kwargs: Any) -> "_IDCapturingVectorStore":  # type: ignore[override]
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
        Use this for debug and testing ONLY, and don't call several index/aindex before draining the buffer"""
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
