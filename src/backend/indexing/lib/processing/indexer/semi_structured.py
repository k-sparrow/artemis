from typing import List, Sequence, override

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.backend.indexing.lib.processing.indexer.base import Indexer
from src.backend.indexing.lib.processing.types import SplitChunks


__all__ = [
    "SemiStructuredIndexer",
]


class SemiStructuredIndexer(Indexer[Sequence[Document], SplitChunks]):
    """Document indexer for semi-structured RAG pipelines.

    This indexer separates documents into text and table content, then chunks
    each separately. The separation optimizes downstream upserting by preventing
    the upserter from having to re-classify content types.

    Documents are classified based on their metadata:
    - Documents with metadata["type"] == "table" are treated as table content
    - All other documents are treated as text content
    """

    def __init__(
        self,
        chunk_size: int = 1024,
        chunk_overlap: int = 100,
        table_chunk_size: int = 2048,
        table_chunk_overlap: int = 0,
    ):
        """Initialize the SemiStructuredIndexer.

        Args:
            chunk_size: Maximum size of each text chunk in characters.
            chunk_overlap: Number of characters to overlap between text chunks.
            table_chunk_size: Maximum size of each table chunk in characters.
            table_chunk_overlap: Number of characters to overlap between table chunks.
        """
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._table_chunk_size = table_chunk_size
        self._table_chunk_overlap = table_chunk_overlap

        self._text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
        self._table_splitter = RecursiveCharacterTextSplitter(
            chunk_size=table_chunk_size,
            chunk_overlap=table_chunk_overlap,
        )

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def chunk_overlap(self) -> int:
        return self._chunk_overlap

    @property
    def table_chunk_size(self) -> int:
        return self._table_chunk_size

    @property
    def table_chunk_overlap(self) -> int:
        return self._table_chunk_overlap

    def _classify_documents(
        self, documents: Sequence[Document]
    ) -> tuple[List[Document], List[Document]]:
        """Classify documents into text and table content.

        Args:
            documents: Documents to classify

        Returns:
            Tuple of (text_documents, table_documents)
        """
        text_docs: List[Document] = []
        table_docs: List[Document] = []

        for doc in documents:
            if doc.metadata.get("type") == "table":
                table_docs.append(doc)
            else:
                text_docs.append(doc)

        return text_docs, table_docs

    @override
    def process(self, data: Sequence[Document]) -> SplitChunks:
        """Process documents by separating and chunking text and table content.

        Args:
            data: Raw documents to process

        Returns:
            SplitChunks containing separated text and table chunks
        """
        text_docs, table_docs = self._classify_documents(data)

        text_chunks = self._text_splitter.split_documents(text_docs)
        table_chunks = self._table_splitter.split_documents(table_docs)

        return SplitChunks(text_chunks=text_chunks, table_chunks=table_chunks)

    @override
    async def aprocess(self, data: Sequence[Document]) -> SplitChunks:
        """Process documents asynchronously.

        Note: Text splitting is CPU-bound, so this delegates to the sync method.

        Args:
            data: Raw documents to process

        Returns:
            SplitChunks containing separated text and table chunks
        """
        return self.process(data)
