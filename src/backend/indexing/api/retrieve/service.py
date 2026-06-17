# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

from typing import Optional

from langchain_core.runnables import ConfigurableField

from langchain_core.documents.compressor import BaseDocumentCompressor
from langchain_core.stores import ByteStore

from src.lib.core.ingestion.pipeline.parent_page import PARENT_ID_FIELD
from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter
from src.lib.core.retrieval.parent_page import ParentPageRetriever
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
    byte_store: Optional[ByteStore] = None,
) -> None:
    global _retriever
    # Build the parent-page decorator HERE (where the byte store lives), bound to
    # the store + the index↔retrieve field contract; NamespaceRetriever re-bases
    # it onto each request's namespace-scoped retriever. None ⇒ no page store
    # configured ⇒ parent-page dereference simply unavailable.
    parent_page_retriever = (
        ParentPageRetriever(byte_store=byte_store, id_key=PARENT_ID_FIELD)
        if byte_store is not None
        else None
    )
    _retriever = NamespaceRetriever(
        namespace_id=None,
        retrieval_adapter=retrieval_adapter,
        reranker=reranker,
        candidates_multiplier=candidates_multiplier,
        parent_page_retriever=parent_page_retriever,
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
