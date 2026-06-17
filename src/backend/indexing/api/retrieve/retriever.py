# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

from typing import Any, List, Optional

from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.multi_vector import SearchType
from langchain_core.documents import Document
from langchain_core.documents.compressor import BaseDocumentCompressor
from langchain_core.retrievers import BaseRetriever
from langchain_core.runnables import RunnableSerializable
from langchain_core.runnables.config import RunnableConfig
from pydantic import ConfigDict

from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter
from src.lib.core.retrieval.params import RetrievalParams, build_qdrant_filter
from src.lib.core.retrieval.parent_page import ParentPageRetriever


__all__ = [
    "NamespaceRetriever",
]


class NamespaceRetriever(RunnableSerializable):
    """LangServe-compatible retriever scoped to a single tenant namespace.

    ``retrieval_adapter`` (a ``BaseRetrievalAdapter`` subclass) owns the
    vectorstore and determines the retrieval algorithm (simple vector search,
    multi-vector, etc.).  Per-request, a Qdrant filter is built from
    ``namespace_id`` (required), ``group_id``, and ``doc_id`` (both optional)
    and injected into ``search_kwargs`` before invoking the adapter.

    When a ``reranker`` is supplied, results are re-ranked via a
    ``ContextualCompressionRetriever`` wrapping the base retriever.
    ``candidates_multiplier`` controls how many raw candidates are fetched
    before re-ranking.

    Parent-page dereference is a PER-REQUEST choice: when ``configurable`` sets
    ``return_parents`` (and a ``parent_page_retriever`` was supplied), the
    composed base (+reranker) retriever is wrapped by re-basing that store-bound
    ``ParentPageRetriever`` onto it — the outermost layer — so the response is the
    chunks' parent pages instead of the chunks.  The decorator is built **outside**
    (in the retrieve service wiring, where the page byte store lives) and threaded
    in here; this retriever only owns the namespace scoping + composition, never
    the byte store.  It composes the same way the reranker does (``… if … else
    base``) and requires no re-embed: indexing always stamps ``parent_id`` and
    caches the pages.  This deref is async-only (the page byte store is
    async-native), so it is available through :meth:`ainvoke`.
    """

    namespace_id: Optional[str] = None
    retrieval_adapter: BaseRetrievalAdapter
    reranker: Optional[BaseDocumentCompressor] = None
    candidates_multiplier: int = 10
    parent_page_retriever: Optional[ParentPageRetriever] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _build_base_retriever(self, configurable: dict) -> BaseRetriever:
        """Compose the namespace-scoped base retriever (+ optional reranker).

        Owns the per-request param plumbing shared by sync and async paths; the
        parent-page wrap (async-only) is applied by the caller on top.
        """
        namespace_id = self.namespace_id or configurable.get("namespace_id")
        if not namespace_id:
            raise ValueError("namespace_id is required in config.configurable")

        k = int(configurable.get("k", 10))
        k_candidates = k * self.candidates_multiplier if self.reranker else k

        params = RetrievalParams(
            namespace_id=namespace_id,
            k=k_candidates,
            group_id=configurable.get("group_id"),
            doc_id=configurable.get("doc_id"),
        )
        ns_filter = build_qdrant_filter(params)
        base = self.retrieval_adapter.get_retriever(
            search_type=SearchType.similarity,
            search_kwargs={"k": k_candidates, "filter": ns_filter},
        )
        return (
            ContextualCompressionRetriever(
                base_compressor=self.reranker, base_retriever=base
            )
            if self.reranker
            else base
        )

    def invoke(
        self, input: str, config: Optional[RunnableConfig] = None, **kwargs: Any
    ) -> List[Document]:
        configurable = (config or {}).get("configurable", {})
        return self._build_base_retriever(configurable).invoke(input, config=config)

    async def ainvoke(
        self, input: str, config: Optional[RunnableConfig] = None, **kwargs: Any
    ) -> List[Document]:
        configurable = (config or {}).get("configurable", {})
        retriever = self._build_base_retriever(configurable)
        if (
            configurable.get("return_parents")
            and self.parent_page_retriever is not None
        ):
            # Outermost layer: rerank chunks first (inside ``retriever``), then
            # dereference each surviving chunk to its parent page. Re-base the
            # store-bound decorator onto this request's base (+reranker) retriever.
            retriever = self.parent_page_retriever.model_copy(
                update={"base_retriever": retriever}
            )
        return await retriever.ainvoke(input, config=config)
