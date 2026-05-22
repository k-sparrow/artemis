from __future__ import annotations

from langchain_core.retrievers import BaseRetriever

from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore
from src.lib.core.retrieval.params import RetrievalParams, build_qdrant_filter


__all__ = [
    "QdrantVectorStoreRetrieverAdapter",
]


class QdrantVectorStoreRetrieverAdapter:
    """Retriever factory that wraps a ``QdrantVectorStore``.

    The vectorstore (and its baked-in retrieval mode — dense, hybrid, or
    multi_stage) is created once at service startup.  ``get_retriever``
    constructs a fresh ``VectorStoreRetriever`` per request by injecting
    the namespace filter and top-k from ``RetrievalParams`` into
    ``search_kwargs``.

    The filter propagates into every prefetch query inside the vectorstore,
    so hybrid and multi_stage modes are namespace-scoped correctly.
    """

    def __init__(self, vectorstore: QdrantVectorStore) -> None:
        self._vs = vectorstore

    def get_retriever(self, params: RetrievalParams) -> BaseRetriever:
        """Return a retriever scoped to the given ``RetrievalParams``.

        The ``filter`` and ``k`` are injected per-request; retrieval mode
        was fixed when the vectorstore was constructed.
        """
        ns_filter = build_qdrant_filter(params)
        return self._vs.as_retriever(search_kwargs={"k": params.k, "filter": ns_filter})
