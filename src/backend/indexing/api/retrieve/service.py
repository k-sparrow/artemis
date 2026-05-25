# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

from typing import Optional

from langchain_core.runnables import ConfigurableField

from langchain_core.documents.compressor import BaseDocumentCompressor

from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter
from src.backend.indexing.api.retrieve.retriever import NamespaceRetriever


__all__ = [
    "initialize",
    "get_retriever",
]

# Module-level singleton, populated by ``initialize()`` during FastAPI lifespan.
#
# Initialising here at module load time is not safe: connecting to Qdrant and
# the TEI embeddings service requires a running event loop and live network
# endpoints that are not available during import.  FastAPI's lifespan context
# manager is the correct hook — it runs inside the async event loop after the
# server is ready and tears down cleanly on shutdown.
#
# Keeping the singleton here (not in router.py) lets other parts of the system
# — a future /catalog route, a health check, a Celery task — call
# ``get_retriever()`` directly without going through the HTTP layer.
_retriever: Optional[NamespaceRetriever] = None


def initialize(
    retrieval_adapter: BaseRetrievalAdapter,
    reranker: Optional[BaseDocumentCompressor],
    candidates_multiplier: int,
) -> None:
    global _retriever
    _retriever = NamespaceRetriever(
        namespace_id=None,
        retrieval_adapter=retrieval_adapter,
        reranker=reranker,
        candidates_multiplier=candidates_multiplier,
    ).configurable_fields(
        namespace_id=ConfigurableField(
            id="namespace_id",
            name="Namespace ID",
            description="Tenant namespace to scope retrieval to.",
            annotation=str,
            is_shared=True,
        ),
    )


def get_retriever() -> NamespaceRetriever:
    if _retriever is None:
        raise RuntimeError("Retriever not initialized — lifespan has not run yet")
    return _retriever
