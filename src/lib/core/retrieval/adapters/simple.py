from typing import Any, Dict

from typing_extensions import override

from langchain.retrievers.multi_vector import SearchType
from langchain_core.vectorstores import VectorStore, VectorStoreRetriever

from src.lib.core.retrieval.adapters.base import BaseRetrievalAdapter


__all__ = [
    "SimpleVectorStoreRetrieverAdapter",
]


class SimpleVectorStoreRetrieverAdapter(BaseRetrievalAdapter):
    def __init__(self, vectorstore: VectorStore, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._vs: VectorStore = vectorstore

    @override
    def get_retriever(
        self, search_type: SearchType, search_kwargs: Dict[str, Any]
    ) -> VectorStoreRetriever:
        """
        Get a MultiVectorRetriever specified with search kwargs

        Sets the search kwargs to the retriever
        """
        return self._vs.as_retriever(
            search_kwargs=search_kwargs,
            search_type=search_type,
        )
