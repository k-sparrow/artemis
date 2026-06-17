# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Parent-page dereference as a composable retriever decorator.

``ParentPageRetriever`` **wraps an inner ``BaseRetriever``** (dense / hybrid /
multi-stage, optionally already reranked) and dereferences each returned chunk's
parent page — exactly the way ``ContextualCompressionRetriever`` composes a
reranker over a base retriever. It runs the inner retriever, reads ``id_key``
(``"parent_id"``) off each chunk, deduplicates (N chunks → their parent pages),
and ``mget``s the pages from a byte store, returning the pages.

Decoupled: it knows nothing about the indexing algorithm — only that the chunks
it receives carry a ``parent_id`` and that the byte store holds the pages under
those keys. The matching ``id_key`` is the one contract with the index side; the
caller passes it (the backend wiring pins it to the index decorator's
``PARENT_ID_FIELD``). Async-only — the byte store (S3) is async-native.

``base_retriever`` may be supplied at construction (standalone use) **or** left
unset and injected at compose time by a host that builds its inner retriever per
request — e.g. the backend ``NamespaceRetriever`` re-bases a store-bound instance
via ``model_copy(update={"base_retriever": ...})``. It must be set before invoke.
"""

from __future__ import annotations

from typing import List, Optional

from langchain.storage import create_kv_docstore
from langchain_core.callbacks import (
    AsyncCallbackManagerForRetrieverRun,
    CallbackManagerForRetrieverRun,
)
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_core.stores import ByteStore
from pydantic import ConfigDict

__all__ = ["ParentPageRetriever"]


class ParentPageRetriever(BaseRetriever):
    """Dereference chunk → parent page on top of any base retriever."""

    byte_store: ByteStore
    base_retriever: Optional[BaseRetriever] = None
    id_key: str = "parent_id"

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def _aget_relevant_documents(
        self, query: str, *, run_manager: AsyncCallbackManagerForRetrieverRun
    ) -> List[Document]:
        if self.base_retriever is None:
            raise RuntimeError(
                "ParentPageRetriever.base_retriever is not set — supply it at "
                "construction or re-base before invoking."
            )
        chunks = await self.base_retriever.ainvoke(
            query, config={"callbacks": run_manager.get_child()}
        )
        # Deduplicate parent ids, order-stable (N chunks → fewer parent pages).
        ids: List[str] = []
        for chunk in chunks:
            parent_id = chunk.metadata.get(self.id_key)
            if parent_id is not None and parent_id not in ids:
                ids.append(parent_id)
        if not ids:
            return []
        docstore = create_kv_docstore(self.byte_store)
        pages = await docstore.amget(ids)
        return [page for page in pages if page is not None]

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        raise NotImplementedError("ParentPageRetriever is async-only; use ainvoke")
