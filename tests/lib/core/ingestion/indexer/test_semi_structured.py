"""Tests for SemiStructuredIndexer."""

import pytest
from src.lib.core.ingestion.indexer.semi_structured import SemiStructuredIndexer
from src.lib.core.ingestion.types import SplitChunks


class TestSemiStructuredIndexer:
    """Test suite for SemiStructuredIndexer."""

    def test_initialization(self):
        """Test indexer initialization with default parameters."""
        indexer = SemiStructuredIndexer()
        assert indexer.chunk_size == 1024
        assert indexer.chunk_overlap == 100
        assert indexer.table_chunk_size == 2048
        assert indexer.table_chunk_overlap == 0

    def test_initialization_custom_params(self):
        """Test indexer initialization with custom parameters."""
        indexer = SemiStructuredIndexer(
            chunk_size=512,
            chunk_overlap=50,
            table_chunk_size=1024,
            table_chunk_overlap=10,
        )
        assert indexer.chunk_size == 512
        assert indexer.chunk_overlap == 50
        assert indexer.table_chunk_size == 1024
        assert indexer.table_chunk_overlap == 10

    def test_process_text_only(self, sample_documents):
        """Test processing documents without tables."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        # Use first two docs (no tables)
        result = indexer.process(sample_documents[:2])

        assert isinstance(result, SplitChunks)
        assert len(result.text_chunks) > 0
        assert len(result.table_chunks) == 0

    def test_process_with_tables(self, sample_documents):
        """Test processing documents with tables."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        result = indexer.process(sample_documents)  # Includes table doc

        assert isinstance(result, SplitChunks)
        assert len(result.text_chunks) > 0
        assert len(result.table_chunks) > 0

        # Verify table chunks have type=table metadata
        for doc in result.table_chunks:
            assert doc.metadata.get("type") == "table"

    def test_process_tables_only(self, sample_documents):
        """Test processing only table documents."""
        indexer = SemiStructuredIndexer()
        # Use only the table document
        result = indexer.process([sample_documents[2]])

        assert isinstance(result, SplitChunks)
        assert len(result.text_chunks) == 0
        assert len(result.table_chunks) >= 1

    @pytest.mark.asyncio
    async def test_aprocess_mixed_content(self, sample_documents):
        """Test async processing with mixed content."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        result = await indexer.aprocess(sample_documents)

        assert isinstance(result, SplitChunks)
        assert len(result.text_chunks) > 0
        assert len(result.table_chunks) > 0

    def test_process_empty_list(self):
        """Test processing an empty document list."""
        indexer = SemiStructuredIndexer()
        result = indexer.process([])

        assert isinstance(result, SplitChunks)
        assert len(result.text_chunks) == 0
        assert len(result.table_chunks) == 0

    def test_metadata_preserved(self, sample_documents):
        """Test that metadata is preserved during chunking."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        result = indexer.process(sample_documents)

        # Check text chunks
        for doc in result.text_chunks:
            assert "source" in doc.metadata
            assert "page" in doc.metadata

        # Check table chunks
        for doc in result.table_chunks:
            assert "source" in doc.metadata
            assert "type" in doc.metadata
            assert doc.metadata["type"] == "table"

    def test_separate_chunking_strategies(self, sample_documents):
        """Test that text and tables use different chunking strategies."""
        # Use different chunk sizes for text and tables
        indexer = SemiStructuredIndexer(
            chunk_size=30,
            chunk_overlap=0,
            table_chunk_size=200,
            table_chunk_overlap=0,
        )
        result = indexer.process(sample_documents)

        # Text chunks should be smaller
        for doc in result.text_chunks:
            assert len(doc.page_content) <= 30 + 100  # Buffer for word boundaries

        # Table chunks can be larger
        # (Just verify they exist, exact size depends on content)
        assert len(result.table_chunks) > 0
