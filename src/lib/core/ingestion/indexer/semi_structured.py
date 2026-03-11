from typing import List, Sequence

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from typing_extensions import override

from src.lib.core.ingestion.indexer.base import Indexer
from src.lib.core.ingestion.types import SplitChunks


__all__ = [
    "SemiStructuredIndexer",
]


class SemiStructuredIndexer(Indexer[Sequence[Document], SplitChunks]):
    """Document indexer for semi-structured RAG pipelines.

    Separates documents into text and table content, then processes each
    according to its type:

    - **Text** is always chunked via ``RecursiveCharacterTextSplitter``.
    - **Tables** are kept whole by default (``split_tables=False``), which is
      correct for the multi-vector RAG pattern where each table is summarized
      as a single semantic unit before embedding.  Pass ``split_tables=True``
      to chunk oversized tables through a second splitter instead.

    Documents are classified by ``metadata["type"] == "table"``; everything
    else is treated as text.

    Returns a :class:`SplitChunks` with the classified and (optionally) split
    chunks.  Summarization is the responsibility of the upserter.
    """

    def __init__(
        self,
        chunk_size: int = 1024,
        chunk_overlap: int = 100,
        table_chunk_size: int = 2048,
        table_chunk_overlap: int = 0,
        split_tables: bool = False,
    ):
        """Initialize the SemiStructuredIndexer.

        Args:
            chunk_size: Maximum size of each text chunk in characters.
            chunk_overlap: Number of characters to overlap between text chunks.
            table_chunk_size: Maximum size of each table chunk in characters.
                Only used when ``split_tables=True``.
            table_chunk_overlap: Number of characters to overlap between table
                chunks.  Only used when ``split_tables=True``.
            split_tables: When ``False`` (default), table documents are passed
                through unchanged — one document per table.  When ``True``,
                tables are split with the table splitter (useful for direct-
                mode pipelines that embed raw table text without summarization).
        """
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._table_chunk_size = table_chunk_size
        self._table_chunk_overlap = table_chunk_overlap
        self._split_tables = split_tables

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

    @property
    def split_tables(self) -> bool:
        return self._split_tables

    def _classify_documents(
        self, documents: Sequence[Document]
    ) -> tuple[List[Document], List[Document]]:
        """Classify documents into text and table content."""
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
        text_docs, table_docs = self._classify_documents(data)

        text_chunks = self._text_splitter.split_documents(text_docs)
        table_chunks = (
            self._table_splitter.split_documents(table_docs)
            if self._split_tables
            else list(table_docs)
        )

        return SplitChunks(text_chunks=text_chunks, table_chunks=table_chunks)

    @override
    async def aprocess(self, data: Sequence[Document]) -> SplitChunks:
        # no true async implementation for this
        return self.process(data=data)
