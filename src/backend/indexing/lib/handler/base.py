from abc import ABC, abstractmethod
import asyncio

from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

__all__ = [
    "BaseVectorHandler",
]


class BaseVectorHandler(ABC):
    embeddings: Embeddings
    vectorstore: VectorStore

    def __init__(self, embeddings: Embeddings, eager: bool = False, **kwargs):
        self.embeddings = embeddings
        self.kwargs = kwargs
        self.vectorstore = self.create() if eager else None

    def create(self) -> VectorStore:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.acreate())

    @abstractmethod
    async def acreate(self) -> VectorStore:
        pass

    @abstractmethod
    async def aclose(self):
        pass
