from typing import Any, Dict

from typing_extensions import override

from langchain.retrievers import MultiVectorRetriever
from langchain.retrievers.multi_vector import SearchType
from langchain_core.stores import BaseStore
from langchain_core.vectorstores import VectorStore

from src.lib.core.retrieval.adapters.base import BaseRertievalAdapter


__all__ = [
    "SemiStructuredRAGRetrievalAdapter",
]


class SemiStructuredRAGRetrievalAdapter(BaseRertievalAdapter):
    def __init__(
        self,
        vectorstore: VectorStore,
        docstore: BaseStore,
        id_key: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._vs = vectorstore
        self._ds = docstore
        self._id_key = id_key

    @override
    def get_retriever(
        self, search_type: SearchType, search_kwargs: Dict[str, Any]
    ) -> MultiVectorRetriever:
        """
        Get a MultiVectorRetriever specified with search kwargs

        Sets the search kwargs to the retriever
        """
        return MultiVectorRetriever(
            vectorstore=self._vs,
            byte_store=self._ds,
            id_key=self._id_key,
            search_kwargs=search_kwargs,
            search_type=search_type,
        )
