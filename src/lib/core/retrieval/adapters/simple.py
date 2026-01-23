from typing import Dict, Any, override

from langchain.retrievers.multi_vector import SearchType
from langchain_core.vectorstores import VectorStore, VectorStoreRetriever

from src.lib.core.retrieval.adapters.base import BaseRertievalAdapter


__all__ = [
    "SimpleVectorStoreRetrieverAdapter",
]


class SimpleVectorStoreRetrieverAdapter(BaseRertievalAdapter):
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
