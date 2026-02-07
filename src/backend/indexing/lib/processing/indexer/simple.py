from typing import List, Sequence, override

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.backend.indexing.lib.processing.indexer.base import Indexer


__all__ = [
    "SimpleIndexer",
]


class SimpleIndexer(Indexer[Sequence[Document], List[Document]]):
    """Simple document indexer using recursive character text splitting.

    This indexer chunks documents using LangChain's RecursiveCharacterTextSplitter,
    which attempts to split on natural boundaries (paragraphs, sentences, words)
    while respecting the specified chunk size and overlap.
    """

    def __init__(self, chunk_size: int = 1024, chunk_overlap: int = 100):
        """Initialize the SimpleIndexer.

        Args:
            chunk_size: Maximum size of each chunk in characters.
            chunk_overlap: Number of characters to overlap between chunks.
        """
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )

    @override
    def process(self, documents: Sequence[Document]) -> List[Document]:
        """Process documents by splitting them into chunks.

        Args:
            documents: Raw documents to process

        Returns:
            List of chunked documents
        """
        return self._splitter.split_documents(list(documents))

    @override
    async def aprocess(self, documents: Sequence[Document]) -> List[Document]:
        """Process documents asynchronously.

        Note: Text splitting is CPU-bound, so this delegates to the sync method.

        Args:
            documents: Raw documents to process

        Returns:
            List of chunked documents
        """
        return self.process(documents)
