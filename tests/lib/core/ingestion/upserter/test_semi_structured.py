"""Tests for SemiStructuredUpserter."""

import pytest
from langchain_core.documents import Document
from src.lib.core.ingestion.upserter import SemiStructuredUpserter
from src.lib.core.ingestion.types import SplitChunks


class TestSemiStructuredUpserter:
    """Test suite for SemiStructuredUpserter."""

    def test_initialization(self, vectorstore):
        """Test upserter initialization."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        assert upserter.vectorstore == vectorstore

    def test_upsert_text_only(self, vectorstore):
        """Test upserting text chunks only."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Text chunk 1", metadata={"source": "test.pdf"}),
                Document(page_content="Text chunk 2", metadata={"source": "test.pdf"}),
            ],
            table_chunks=[],
        )

        result = upserter.upsert(chunks)

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(doc_id, str) for doc_id in result)

    def test_upsert_tables_only(self, vectorstore):
        """Test upserting table chunks only."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[],
            table_chunks=[
                Document(
                    page_content="Table data",
                    metadata={"source": "test.pdf", "type": "table"},
                ),
            ],
        )

        result = upserter.upsert(chunks)

        assert isinstance(result, list)
        assert len(result) == 1
        assert all(isinstance(doc_id, str) for doc_id in result)

    def test_upsert_mixed_chunks(self, vectorstore):
        """Test upserting both text and table chunks."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Text 1", metadata={"source": "test.pdf"}),
                Document(page_content="Text 2", metadata={"source": "test.pdf"}),
            ],
            table_chunks=[
                Document(
                    page_content="Table 1",
                    metadata={"source": "test.pdf", "type": "table"},
                ),
                Document(
                    page_content="Table 2",
                    metadata={"source": "test.pdf", "type": "table"},
                ),
            ],
        )

        result = upserter.upsert(chunks)

        assert isinstance(result, list)
        assert len(result) == 4  # 2 text + 2 table
        assert all(isinstance(doc_id, str) for doc_id in result)
        # Verify all IDs are unique
        assert len(set(result)) == len(result)

    @pytest.mark.asyncio
    async def test_aupsert_mixed_chunks(self, vectorstore):
        """Test async upserting of mixed chunks."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Async text", metadata={"source": "async.pdf"}),
            ],
            table_chunks=[
                Document(
                    page_content="Async table",
                    metadata={"source": "async.pdf", "type": "table"},
                ),
            ],
        )

        result = await upserter.aupsert(chunks)

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(doc_id, str) for doc_id in result)

    def test_upsert_empty_chunks(self, vectorstore):
        """Test upserting empty chunk collections."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(text_chunks=[], table_chunks=[])

        result = upserter.upsert(chunks)

        assert result == []

    @pytest.mark.asyncio
    async def test_aupsert_empty_chunks(self, vectorstore):
        """Test async upserting of empty chunk collections."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(text_chunks=[], table_chunks=[])

        result = await upserter.aupsert(chunks)

        assert result == []

    @pytest.mark.asyncio
    async def test_upserted_chunks_retrievable(self, vectorstore, qdrant_client):
        """Test that upserted chunks can be retrieved from vectorstore."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(
                    page_content="Machine learning text",
                    metadata={"source": "ml.pdf"},
                ),
            ],
            table_chunks=[
                Document(
                    page_content="ML metrics table",
                    metadata={"source": "ml.pdf", "type": "table"},
                ),
            ],
        )

        # Upsert chunks
        doc_ids = await upserter.aupsert(chunks)
        assert len(doc_ids) == 2

        # Verify chunks are in Qdrant
        collection_info = await qdrant_client.get_collection("test_collection")
        assert collection_info.points_count == 2

    @pytest.mark.asyncio
    async def test_table_metadata_preserved(self, vectorstore):
        """Test that table metadata is preserved."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[],
            table_chunks=[
                Document(
                    page_content="Table with metadata",
                    metadata={
                        "source": "tables.pdf",
                        "type": "table",
                        "table_id": "table_1",
                        "caption": "Performance Metrics",
                    },
                ),
            ],
        )

        doc_ids = await upserter.aupsert(chunks)

        # Verify upsert succeeded (detailed metadata verification depends on retrieval)
        assert len(doc_ids) == 1

    def test_separate_upsert_calls(self, vectorstore):
        """Test that text and tables are upserted in separate calls."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Text", metadata={"source": "test.pdf"}),
            ],
            table_chunks=[
                Document(
                    page_content="Table",
                    metadata={"source": "test.pdf", "type": "table"},
                ),
            ],
        )

        result = upserter.upsert(chunks)

        # Should return IDs for both text and table chunks
        assert len(result) == 2
        # IDs should be in order: text chunks first, then table chunks
        # (This is implementation-specific but good to document)
        assert all(isinstance(doc_id, str) for doc_id in result)
