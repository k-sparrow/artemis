from abc import ABC, abstractmethod
from typing import Dict, Any

from langchain.retrievers.multi_vector import SearchType
from langchain_core.retrievers import BaseRetriever


__all__ = [
    "BaseRertievalAdapter",
]


class BaseRetrievalAdapter(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def get_retriever(
        self, search_type: SearchType, search_kwargs: Dict[str, Any]
    ) -> BaseRetriever:
        """
        Returns a retriever adapted to specified search_kwargs
        and retrieval method

        For different retrieval method, please implement in subclasses
        """
