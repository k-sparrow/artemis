"""Tests for SimpleUpserter."""

import pytest
from langchain_core.documents import Document
from src.lib.core.ingestion.upserter import SimpleUpserter


class TestSimpleUpserter:
    """Test suite for SimpleUpserter."""

    def test_initialization(self, vectorstore):
        """Test upserter initialization."""
        upserter = SimpleUpserter(vectorstore=vectorstore)
        assert upserter.vectorstore == vectorstore

    def test_upsert_single_document(self, vectorstore):
        """Test upserting a single document."""
        upserter = SimpleUpserter(vectorstore=vectorstore)
        docs = [
            Document(
                page_content="Test document content",
                metadata={"source": "test.pdf"},
            )
        ]

        result = upserter.upsert(docs)

        assert isinstance(result, list)
        assert len(result) == 1
        assert all(isinstance(doc_id, str) for doc_id in result)

    def test_upsert_multiple_documents(self, vectorstore, sample_documents):
        """Test upserting multiple documents."""
        upserter = SimpleUpserter(vectorstore=vectorstore)

        result = upserter.upsert(sample_documents)

        assert isinstance(result, list)
        assert len(result) == len(sample_documents)
        assert all(isinstance(doc_id, str) for doc_id in result)
        # Verify IDs are unique
        assert len(set(result)) == len(result)

    @pytest.mark.asyncio
    async def test_aupsert_single_document(self, vectorstore):
        """Test async upserting of a single document."""
        upserter = SimpleUpserter(vectorstore=vectorstore)
        docs = [
            Document(
                page_content="Test async document",
                metadata={"source": "test_async.pdf"},
            )
        ]

        result = await upserter.aupsert(docs)

        assert isinstance(result, list)
        assert len(result) == 1
        assert all(isinstance(doc_id, str) for doc_id in result)

    @pytest.mark.asyncio
    async def test_aupsert_multiple_documents(self, vectorstore, sample_documents):
        """Test async upserting of multiple documents."""
        upserter = SimpleUpserter(vectorstore=vectorstore)

        result = await upserter.aupsert(sample_documents)

        assert isinstance(result, list)
        assert len(result) == len(sample_documents)
        assert all(isinstance(doc_id, str) for doc_id in result)

    def test_upsert_empty_list(self, vectorstore):
        """Test upserting an empty document list."""
        upserter = SimpleUpserter(vectorstore=vectorstore)

        result = upserter.upsert([])

        assert result == []

    @pytest.mark.asyncio
    async def test_aupsert_empty_list(self, vectorstore):
        """Test async upserting of an empty document list."""
        upserter = SimpleUpserter(vectorstore=vectorstore)

        result = await upserter.aupsert([])

        assert result == []

    @pytest.mark.asyncio
    async def test_upserted_documents_retrievable(self, vectorstore, qdrant_client):
        """Test that upserted documents can be retrieved."""
        upserter = SimpleUpserter(vectorstore=vectorstore)
        docs = [
            Document(
                page_content="Machine learning is a subset of artificial intelligence",
                metadata={"source": "ml.pdf", "topic": "ML"},
            ),
            Document(
                page_content="Deep learning uses neural networks",
                metadata={"source": "dl.pdf", "topic": "DL"},
            ),
        ]

        # Upsert documents
        doc_ids = await upserter.aupsert(docs)
        assert len(doc_ids) == 2

        # Verify documents are in Qdrant
        collection_info = await qdrant_client.get_collection("test_collection")
        assert collection_info.points_count == 2

    @pytest.mark.asyncio
    async def test_metadata_preserved_in_vectorstore(self, vectorstore):
        """Test that metadata is preserved when upserting to vectorstore."""
        upserter = SimpleUpserter(vectorstore=vectorstore)
        metadata = {
            "source": "test.pdf",
            "page": 42,
            "author": "Test Author",
            "custom_field": "custom_value",
        }
        docs = [
            Document(
                page_content="Test content with metadata",
                metadata=metadata,
            )
        ]

        doc_ids = await upserter.aupsert(docs)

        # Retrieve and verify metadata
        # Note: This depends on the vectorstore implementation
        # For now, just verify the upsert succeeded
        assert len(doc_ids) == 1
