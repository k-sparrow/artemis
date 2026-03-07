"""Tests for SemiStructuredUpserter."""

import pytest
from langchain_core.documents import Document

from src.lib.core.ingestion.types import SplitChunks, UpsertResult
from src.lib.core.ingestion.upserter import SemiStructuredUpserter


class TestSemiStructuredUpserter:
    """Test suite for SemiStructuredUpserter."""

    def test_initialization(self, vectorstore):
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        assert upserter.vectorstore == vectorstore

    def test_upsert_text_only(self, vectorstore):
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Text chunk 1", metadata={"source": "test.pdf"}),
                Document(page_content="Text chunk 2", metadata={"source": "test.pdf"}),
            ],
            table_chunks=[],
        )

        result = upserter.upsert(chunks)

        assert isinstance(result, UpsertResult)
        assert result.num_added == 2
        assert len(result.ids) == 2
        assert all(isinstance(doc_id, str) for doc_id in result.ids)

    def test_upsert_tables_only(self, vectorstore):
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

        assert isinstance(result, UpsertResult)
        assert result.num_added == 1
        assert len(result.ids) == 1
        assert all(isinstance(doc_id, str) for doc_id in result.ids)

    def test_upsert_mixed_chunks(self, vectorstore):
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

        assert isinstance(result, UpsertResult)
        assert result.num_added == 4
        assert len(result.ids) == 4
        assert all(isinstance(doc_id, str) for doc_id in result.ids)
        assert len(set(result.ids)) == 4

    @pytest.mark.asyncio
    async def test_aupsert_mixed_chunks(self, vectorstore):
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

        assert isinstance(result, UpsertResult)
        assert result.num_added == 2
        assert len(result.ids) == 2
        assert all(isinstance(doc_id, str) for doc_id in result.ids)

    def test_upsert_empty_chunks(self, vectorstore):
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)

        result = upserter.upsert(SplitChunks(text_chunks=[], table_chunks=[]))

        assert isinstance(result, UpsertResult)
        assert result == UpsertResult()

    @pytest.mark.asyncio
    async def test_aupsert_empty_chunks(self, vectorstore):
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)

        result = await upserter.aupsert(SplitChunks(text_chunks=[], table_chunks=[]))

        assert isinstance(result, UpsertResult)
        assert result == UpsertResult()

    @pytest.mark.asyncio
    async def test_upserted_chunks_retrievable(self, vectorstore, qdrant_client):
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(
                    page_content="Machine learning text", metadata={"source": "ml.pdf"}
                ),
            ],
            table_chunks=[
                Document(
                    page_content="ML metrics table",
                    metadata={"source": "ml.pdf", "type": "table"},
                ),
            ],
        )

        result = await upserter.aupsert(chunks)
        assert len(result.ids) == 2

        count = await qdrant_client.count(vectorstore.collection_name)
        assert count.count == 2

    @pytest.mark.asyncio
    async def test_deduplication_with_record_manager(self, vectorstore, record_manager):
        """With a record_manager, repeated upsert skips unchanged chunks."""
        upserter = SemiStructuredUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Dedup text", metadata={"source": "dedup.pdf"}),
            ],
            table_chunks=[],
        )

        first = await upserter.aupsert(chunks)
        second = await upserter.aupsert(chunks)

        assert first.num_added == 1
        assert second.num_added == 0
        assert second.num_skipped == 1

    @pytest.mark.asyncio
    async def test_table_metadata_preserved(self, vectorstore):
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
                    },
                ),
            ],
        )

        result = await upserter.aupsert(chunks)

        assert result.num_added == 1
        assert len(result.ids) == 1

    def test_separate_upsert_calls(self, vectorstore):
        """Text and table chunks are upserted in separate add_documents calls."""
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        chunks = SplitChunks(
            text_chunks=[
                Document(page_content="Text", metadata={"source": "test.pdf"})
            ],
            table_chunks=[
                Document(
                    page_content="Table",
                    metadata={"source": "test.pdf", "type": "table"},
                )
            ],
        )

        result = upserter.upsert(chunks)

        assert result.num_added == 2
        assert len(result.ids) == 2
        assert all(isinstance(doc_id, str) for doc_id in result.ids)
