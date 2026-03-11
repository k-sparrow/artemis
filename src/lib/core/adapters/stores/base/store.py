# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Generic DocumentIndex backed by any ByteStore.

Provides :class:`StoreDocumentIndex`, a concrete :class:`DocumentIndex`
implementation that delegates Document storage to any
``BaseStore[str, bytes]`` (LangChain's ``ByteStore``).  Document
serialization is handled by ``create_kv_docstore``.

SQL-specific concerns (schema creation, teardown, engine factories) live
in the :class:`~src.lib.core.adapters.stores.sql.store.SQLDocumentIndex`
subclass.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any, List, Optional

from langchain.storage import create_kv_docstore
from langchain_core.callbacks.manager import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.indexing.base import DeleteResponse, DocumentIndex, UpsertResponse
from langchain_core.stores import ByteStore

__all__ = ["StoreDocumentIndex"]


class StoreDocumentIndex(DocumentIndex):
    """A :class:`DocumentIndex` backed by any ``ByteStore``.

    The store is responsible for persisting bytes; this class handles the
    bytes ↔ :class:`Document` serialization layer via
    ``create_kv_docstore``.

    Subclasses that need store-specific initialization (e.g. SQL schema
    creation) should override :meth:`_setup` and :meth:`_asetup`.  Both
    hooks are called at the start of every mutating operation.

    Attributes:
        store: The underlying byte store.
    """

    store: ByteStore

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    def _setup(self) -> None:
        """Called before every sync mutating operation.  No-op by default."""

    async def _asetup(self) -> None:
        """Called before every async mutating operation.  No-op by default."""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_ids_from_items(
        self, items: Sequence[Document], ids: Optional[Sequence[str]] = None
    ) -> List[str]:
        if ids:
            return list(ids)
        if all((hasattr(item, "id") and item.id is not None) for item in items):
            return [item.id for item in items]
        return [str(uuid.uuid4()) for _ in items]

    # ------------------------------------------------------------------
    # DocumentIndex interface
    # ------------------------------------------------------------------

    def upsert(self, items: Sequence[Document], /, **kwargs: Any) -> UpsertResponse:
        self._setup()
        docstore = create_kv_docstore(self.store)
        ids = self._extract_ids_from_items(items, kwargs.get("ids"))
        docstore.mset(list(zip(ids, items)))
        return UpsertResponse(succeeded=ids, failed=[])

    async def aupsert(
        self, items: Sequence[Document], /, **kwargs: Any
    ) -> UpsertResponse:
        await self._asetup()
        docstore = create_kv_docstore(self.store)
        ids = self._extract_ids_from_items(items, kwargs.get("ids"))
        await docstore.amset(list(zip(ids, items)))
        return UpsertResponse(succeeded=ids, failed=[])

    def get(self, ids: Sequence[str], /, **kwargs: Any) -> List[Document]:
        self._setup()
        docstore = create_kv_docstore(self.store)
        return docstore.mget(list(ids))  # type: ignore[return-value]

    async def aget(self, ids: Sequence[str], /, **kwargs: Any) -> List[Document]:
        await self._asetup()
        docstore = create_kv_docstore(self.store)
        return await docstore.amget(list(ids))  # type: ignore[return-value]

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> DeleteResponse:
        self._setup()
        docstore = create_kv_docstore(self.store)
        ids = ids or list(self.store.yield_keys())
        docstore.mdelete(ids)
        return DeleteResponse(
            num_deleted=len(ids), succeeded=ids, failed=[], num_failed=0
        )

    async def adelete(
        self, ids: Optional[List[str]] = None, **kwargs: Any
    ) -> DeleteResponse:
        await self._asetup()
        docstore = create_kv_docstore(self.store)
        ids = ids or [key async for key in self.store.ayield_keys()]
        await docstore.amdelete(ids)
        return DeleteResponse(
            num_deleted=len(ids), succeeded=ids, failed=[], num_failed=0
        )

    # Required by BaseRetriever; not used in this context.
    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        return []
