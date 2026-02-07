from typing import List, override

from langchain_core.vectorstores import VectorStore

from src.backend.indexing.lib.processing.types import SplitChunks
from src.backend.indexing.lib.processing.upserter.base import Upserter


__all__ = [
    "SemiStructuredUpserter",
]


class SemiStructuredUpserter(Upserter[SplitChunks, List[str]]):
    """Upserter for semi-structured RAG pipelines.

    Handles pre-split text and table chunks, upserting them to the vectorstore.
    The separation is already done by the indexer, so no re-classification
    is needed here.

    Both text and table chunks are stored in the same vectorstore but can
    be distinguished by their metadata.
    """

    def __init__(self, vectorstore: VectorStore):
        """Initialize the SemiStructuredUpserter.

        Args:
            vectorstore: The vectorstore to upsert documents to.
        """
        self._vectorstore = vectorstore

    @property
    def vectorstore(self) -> VectorStore:
        return self._vectorstore

    @override
    def upsert(self, data: SplitChunks) -> List[str]:
        """Upsert split chunks to the vectorstore synchronously.

        Args:
            data: SplitChunks containing text and table chunks

        Returns:
            List of document IDs that were upserted
        """
        ids: List[str] = []

        if data.text_chunks:
            ids.extend(self._vectorstore.add_documents(data.text_chunks))

        if data.table_chunks:
            ids.extend(self._vectorstore.add_documents(data.table_chunks))

        return ids

    @override
    async def aupsert(self, data: SplitChunks) -> List[str]:
        """Upsert split chunks to the vectorstore asynchronously.

        Args:
            data: SplitChunks containing text and table chunks

        Returns:
            List of document IDs that were upserted
        """
        ids: List[str] = []

        if data.text_chunks:
            text_ids = await self._vectorstore.aadd_documents(data.text_chunks)
            ids.extend(text_ids)

        if data.table_chunks:
            table_ids = await self._vectorstore.aadd_documents(data.table_chunks)
            ids.extend(table_ids)

        return ids
