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
from langchain_core.runnables import RunnableSerializable
from langchain_core.runnables.config import RunnableConfig
from pydantic import ConfigDict

from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter
from src.lib.core.retrieval.params import RetrievalParams, build_qdrant_filter


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
    """

    namespace_id: Optional[str] = None
    retrieval_adapter: BaseRetrievalAdapter
    reranker: Optional[BaseDocumentCompressor] = None
    candidates_multiplier: int = 10

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def invoke(
        self, input: str, config: Optional[RunnableConfig] = None, **kwargs: Any
    ) -> List[Document]:
        configurable = (config or {}).get("configurable", {})
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
        retriever = (
            ContextualCompressionRetriever(
                base_compressor=self.reranker, base_retriever=base
            )
            if self.reranker
            else base
        )
        return retriever.invoke(input, config=config)
