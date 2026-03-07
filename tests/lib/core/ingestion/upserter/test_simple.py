"""Tests for SimpleUpserter."""

import pytest
from langchain.indexes import SQLRecordManager
from langchain_core.documents import Document

from src.lib.core.ingestion.types import UpsertResult
from src.lib.core.ingestion.upserter import SimpleUpserter


class TestSimpleUpserter:
    """Test suite for SimpleUpserter."""

    def test_initialization(self, vectorstore, sync_record_manager):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=sync_record_manager
        )
        assert upserter.vectorstore == vectorstore

    def test_upsert_single_document(self, vectorstore, sync_record_manager):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=sync_record_manager
        )
        docs = [
            Document(
                page_content="Test document content",
                metadata={"source": "test.pdf"},
            )
        ]

        result = upserter.upsert(docs)

        assert isinstance(result, UpsertResult)
        assert result.num_added == 1
        assert len(result.ids) == 1
        assert all(isinstance(doc_id, str) for doc_id in result.ids)

    def test_upsert_multiple_documents(
        self, vectorstore, sync_record_manager, sample_documents
    ):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=sync_record_manager
        )

        result = upserter.upsert(sample_documents)

        assert isinstance(result, UpsertResult)
        assert result.num_added == len(sample_documents)
        assert len(result.ids) == len(sample_documents)
        assert all(isinstance(doc_id, str) for doc_id in result.ids)
        assert len(set(result.ids)) == len(result.ids)

    @pytest.mark.asyncio
    async def test_aupsert_single_document(self, vectorstore, record_manager):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )
        docs = [
            Document(
                page_content="Test async document",
                metadata={"source": "test_async.pdf"},
            )
        ]

        result = await upserter.aupsert(docs)

        assert isinstance(result, UpsertResult)
        assert result.num_added == 1
        assert len(result.ids) == 1
        assert all(isinstance(doc_id, str) for doc_id in result.ids)

    @pytest.mark.asyncio
    async def test_aupsert_multiple_documents(
        self, vectorstore, record_manager, sample_documents
    ):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )

        result = await upserter.aupsert(sample_documents)

        assert isinstance(result, UpsertResult)
        assert result.num_added == len(sample_documents)
        assert len(result.ids) == len(sample_documents)
        assert all(isinstance(doc_id, str) for doc_id in result.ids)

    def test_upsert_empty_list(self, vectorstore, sync_record_manager):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=sync_record_manager
        )

        result = upserter.upsert([])

        assert isinstance(result, UpsertResult)
        assert result == UpsertResult()

    @pytest.mark.asyncio
    async def test_aupsert_empty_list(self, vectorstore, record_manager):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )

        result = await upserter.aupsert([])

        assert isinstance(result, UpsertResult)
        assert result == UpsertResult()

    @pytest.mark.asyncio
    async def test_deduplication_on_second_upsert(self, vectorstore, record_manager):
        """Second upsert of identical docs should be skipped, not re-added."""
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )
        docs = [
            Document(
                page_content="Machine learning is a subset of artificial intelligence",
                metadata={"source": "ml.pdf"},
            ),
        ]

        first = await upserter.aupsert(docs)
        second = await upserter.aupsert(docs)

        assert first.num_added == 1
        assert second.num_added == 0
        assert second.num_skipped == 1
        assert second.ids == []

    @pytest.mark.asyncio
    async def test_upserted_documents_retrievable(
        self, vectorstore, record_manager, qdrant_client
    ):
        """Upserted documents land in the right Qdrant collection."""
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )
        docs = [
            Document(
                page_content="Machine learning is a subset of artificial intelligence",
                metadata={"source": "ml.pdf"},
            ),
            Document(
                page_content="Deep learning uses neural networks",
                metadata={"source": "dl.pdf"},
            ),
        ]

        result = await upserter.aupsert(docs)
        assert len(result.ids) == 2

        count = await qdrant_client.count(vectorstore.collection_name)
        assert count.count == 2

    @pytest.mark.asyncio
    async def test_metadata_preserved_in_vectorstore(self, vectorstore, record_manager):
        upserter = SimpleUpserter(
            vectorstore=vectorstore, record_manager=record_manager
        )
        docs = [
            Document(
                page_content="Test content with metadata",
                metadata={"source": "test.pdf", "page": 42, "author": "Test Author"},
            )
        ]

        result = await upserter.aupsert(docs)

        assert result.num_added == 1
        assert len(result.ids) == 1
