# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from typing import Any, Dict

from typing_extensions import override

from langchain.retrievers import MultiVectorRetriever
from langchain.retrievers.multi_vector import SearchType
from langchain_core.stores import BaseStore
from langchain_core.vectorstores import VectorStore

from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter


__all__ = [
    "SemiStructuredRAGRetrievalAdapter",
]


class SemiStructuredRAGRetrievalAdapter(BaseRetrievalAdapter):
    """Retrieval adapter for the semi-structured multi-vector RAG pattern.

    Wraps LangChain's ``MultiVectorRetriever``, which:

    1. Embeds the query and performs similarity search in the *vectorstore*
       against stored *summaries*.
    2. Reads the ``id_key`` metadata field from the top-k summary documents to
       obtain the original-chunk UUIDs.
    3. Fetches the original chunks from the *byte_store* by those UUIDs and
       returns them as the final context.

    The ``id_key`` and ``byte_store`` must match what was used during ingestion
    by :class:`SemiStructuredUpserter` — use :func:`create_retriever` in
    ``config.py`` to guarantee consistency.
    """

    def __init__(
        self,
        vectorstore: VectorStore,
        byte_store: BaseStore,
        id_key: str,
    ):
        super().__init__()
        self._vs = vectorstore
        self._ds = byte_store
        self._id_key = id_key

    @override
    def get_retriever(
        self, search_type: SearchType, search_kwargs: Dict[str, Any]
    ) -> MultiVectorRetriever:
        return MultiVectorRetriever(
            vectorstore=self._vs,
            byte_store=self._ds,
            id_key=self._id_key,
            search_kwargs=search_kwargs,
            search_type=search_type,
        )
