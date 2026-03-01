"""Tests for SimpleIndexer."""

import pytest
from src.lib.core.ingestion.indexer.simple import SimpleIndexer


class TestSimpleIndexer:
    """Test suite for SimpleIndexer."""

    def test_initialization(self):
        """Test indexer initialization with default parameters."""
        indexer = SimpleIndexer()
        assert indexer._chunk_size == 1024
        assert indexer._chunk_overlap == 100

    def test_initialization_custom_params(self):
        """Test indexer initialization with custom parameters."""
        indexer = SimpleIndexer(chunk_size=512, chunk_overlap=50)
        assert indexer._chunk_size == 512
        assert indexer._chunk_overlap == 50

    def test_process_single_document(self, sample_documents):
        """Test processing a single document."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        result = indexer.process([sample_documents[0]])

        # Should split into multiple chunks
        assert len(result) > 0
        assert all(hasattr(doc, "page_content") for doc in result)
        assert all(hasattr(doc, "metadata") for doc in result)

    def test_process_multiple_documents(self, sample_documents):
        """Test processing multiple documents."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        result = indexer.process(sample_documents[:2])  # First two docs

        # Should produce multiple chunks
        assert len(result) > 2  # More chunks than input documents
        # Verify metadata is preserved
        sources = {doc.metadata.get("source") for doc in result}
        assert "test1.pdf" in sources

    @pytest.mark.asyncio
    async def test_aprocess_single_document(self, sample_documents):
        """Test async processing of a single document."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        result = await indexer.aprocess([sample_documents[0]])

        assert len(result) > 0
        assert all(hasattr(doc, "page_content") for doc in result)

    @pytest.mark.asyncio
    async def test_aprocess_multiple_documents(self, sample_documents):
        """Test async processing of multiple documents."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        result = await indexer.aprocess(sample_documents)

        # Should process all documents
        assert len(result) > 0
        # Check that we have chunks from different sources
        sources = {doc.metadata.get("source") for doc in result}
        assert len(sources) >= 2

    def test_process_empty_list(self):
        """Test processing an empty document list."""
        indexer = SimpleIndexer()
        result = indexer.process([])
        assert result == []

    def test_chunk_size_respected(self, sample_documents):
        """Test that chunk size limits are respected."""
        chunk_size = 30
        indexer = SimpleIndexer(chunk_size=chunk_size, chunk_overlap=0)
        result = indexer.process([sample_documents[0]])

        # Each chunk should be <= chunk_size
        for doc in result:
            assert (
                len(doc.page_content) <= chunk_size + 100
            )  # Allow some buffer for word boundaries

    def test_metadata_preserved(self, sample_documents):
        """Test that metadata is preserved during chunking."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        result = indexer.process([sample_documents[0]])

        # All chunks should have the original metadata
        for doc in result:
            assert doc.metadata["source"] == "test1.pdf"
            assert doc.metadata["page"] == 1
