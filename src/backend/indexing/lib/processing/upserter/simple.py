from typing import List, override

from langchain_core.documents import Document
from langchain_core.vectorstores import VectorStore

from src.backend.indexing.lib.processing.upserter.base import Upserter


__all__ = [
    "SimpleUpserter",
]


class SimpleUpserter(Upserter[List[Document], List[str]]):
    """Simple upserter for standard RAG pipelines.

    Upserts document chunks directly to a vectorstore.
    Returns the list of document IDs that were stored.
    """

    def __init__(self, vectorstore: VectorStore):
        """Initialize the SimpleUpserter.

        Args:
            vectorstore: The vectorstore to upsert documents to.
        """
        self._vectorstore = vectorstore

    @property
    def vectorstore(self) -> VectorStore:
        return self._vectorstore

    @override
    def upsert(self, data: List[Document]) -> List[str]:
        """Upsert documents to the vectorstore synchronously.

        Args:
            data: List of document chunks to upsert

        Returns:
            List of document IDs that were upserted
        """
        if not data:
            return []
        return self._vectorstore.add_documents(data)

    @override
    async def aupsert(self, data: List[Document]) -> List[str]:
        """Upsert documents to the vectorstore asynchronously.

        Args:
            data: List of document chunks to upsert

        Returns:
            List of document IDs that were upserted
        """
        if not data:
            return []
        return await self._vectorstore.aadd_documents(data)
