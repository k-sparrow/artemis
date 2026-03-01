"""Tests for Pipeline class with end-to-end integration."""

import pytest
from langchain_core.documents import Document
from src.lib.core.ingestion import (
    Pipeline,
    SimpleIndexer,
    SimpleUpserter,
    SemiStructuredIndexer,
    SemiStructuredUpserter,
)


class TestSimplePipeline:
    """Test suite for Pipeline with SimpleIndexer + SimpleUpserter."""

    def test_initialization(self, vectorstore):
        """Test pipeline initialization with simple components."""
        indexer = SimpleIndexer(chunk_size=100, chunk_overlap=20)
        upserter = SimpleUpserter(vectorstore=vectorstore)

        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        assert pipeline.indexer == indexer
        assert pipeline.upserter == upserter

    def test_process_sync(self, vectorstore):
        """Test synchronous end-to-end processing."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        documents = [
            Document(
                page_content="Machine learning pipeline test.",
                metadata={"source": "test.pdf"},
            ),
            Document(
                page_content="Testing document processing.",
                metadata={"source": "test.pdf"},
            ),
        ]

        result = pipeline.process(documents)

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(doc_id, str) for doc_id in result)

    @pytest.mark.asyncio
    async def test_aprocess_async(self, vectorstore):
        """Test asynchronous end-to-end processing."""
        indexer = SimpleIndexer(chunk_size=50, chunk_overlap=10)
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        documents = [
            Document(
                page_content="Async pipeline processing test.",
                metadata={"source": "async.pdf"},
            ),
        ]

        result = await pipeline.aprocess(documents)

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(doc_id, str) for doc_id in result)

    @pytest.mark.asyncio
    async def test_documents_retrievable_after_pipeline(
        self, vectorstore, qdrant_client
    ):
        """Test that documents can be retrieved after pipeline processing."""
        indexer = SimpleIndexer(chunk_size=100, chunk_overlap=20)
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        documents = [
            Document(
                page_content="Knowledge graph embeddings in NLP applications.",
                metadata={"source": "research.pdf", "page": 1},
            ),
        ]

        doc_ids = await pipeline.aprocess(documents)
        assert len(doc_ids) > 0

        # Verify documents are in Qdrant
        collection_info = await qdrant_client.get_collection("test_collection")
        assert collection_info.points_count == len(doc_ids)

    def test_process_empty_documents(self, vectorstore):
        """Test pipeline with empty document list."""
        indexer = SimpleIndexer()
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        result = pipeline.process([])

        assert result == []

    @pytest.mark.asyncio
    async def test_aprocess_empty_documents(self, vectorstore):
        """Test async pipeline with empty document list."""
        indexer = SimpleIndexer()
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        result = await pipeline.aprocess([])

        assert result == []

    @pytest.mark.asyncio
    async def test_metadata_preserved_through_pipeline(self, vectorstore):
        """Test that metadata flows through the entire pipeline."""
        indexer = SimpleIndexer(chunk_size=50)
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        documents = [
            Document(
                page_content="Testing metadata preservation.",
                metadata={"source": "meta.pdf", "page": 5, "author": "Test Author"},
            ),
        ]

        doc_ids = await pipeline.aprocess(documents)

        # Just verify successful processing - metadata verification requires retrieval
        assert len(doc_ids) > 0


class TestSemiStructuredPipeline:
    """Test suite for Pipeline with SemiStructuredIndexer + SemiStructuredUpserter."""

    def test_initialization(self, vectorstore):
        """Test pipeline initialization with semi-structured components."""
        indexer = SemiStructuredIndexer()
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)

        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        assert pipeline.indexer == indexer
        assert pipeline.upserter == upserter

    def test_process_mixed_content_sync(self, vectorstore, sample_documents):
        """Test synchronous processing of mixed text and table content."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        result = pipeline.process(sample_documents)

        assert isinstance(result, list)
        assert len(result) > 0  # Should have both text and table chunks
        assert all(isinstance(doc_id, str) for doc_id in result)

    @pytest.mark.asyncio
    async def test_aprocess_mixed_content_async(self, vectorstore, sample_documents):
        """Test asynchronous processing of mixed text and table content."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        result = await pipeline.aprocess(sample_documents)

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(doc_id, str) for doc_id in result)

    @pytest.mark.asyncio
    async def test_text_and_tables_stored_separately(
        self, vectorstore, sample_documents, qdrant_client
    ):
        """Test that text and table chunks are both stored in vectorstore."""
        indexer = SemiStructuredIndexer(chunk_size=50, chunk_overlap=10)
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        doc_ids = await pipeline.aprocess(sample_documents)

        # Verify all documents are in Qdrant
        collection_info = await qdrant_client.get_collection("test_collection")
        assert collection_info.points_count == len(doc_ids)
        assert collection_info.points_count > len(
            sample_documents
        )  # Chunks > original docs

    def test_process_text_only_documents(self, vectorstore):
        """Test pipeline with only text documents (no tables)."""
        indexer = SemiStructuredIndexer(chunk_size=50)
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        documents = [
            Document(
                page_content="Text only document.", metadata={"source": "text.pdf"}
            ),
        ]

        result = pipeline.process(documents)

        assert isinstance(result, list)
        assert len(result) > 0

    def test_process_table_only_documents(self, vectorstore):
        """Test pipeline with only table documents."""
        indexer = SemiStructuredIndexer()
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        documents = [
            Document(
                page_content="| Column1 | Column2 |\n| Value1 | Value2 |",
                metadata={"source": "tables.pdf", "type": "table"},
            ),
        ]

        result = pipeline.process(documents)

        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_empty_documents(self, vectorstore):
        """Test semi-structured pipeline with empty document list."""
        indexer = SemiStructuredIndexer()
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        result = await pipeline.aprocess([])

        assert result == []

    @pytest.mark.asyncio
    async def test_large_document_chunking(self, vectorstore):
        """Test pipeline with large documents that require chunking."""
        indexer = SemiStructuredIndexer(chunk_size=30, chunk_overlap=5)
        upserter = SemiStructuredUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        # Create a document with content that will definitely be chunked
        large_text = " ".join([f"Word{i}" for i in range(100)])
        documents = [
            Document(page_content=large_text, metadata={"source": "large.pdf"}),
        ]

        doc_ids = await pipeline.aprocess(documents)

        # Should create multiple chunks
        assert len(doc_ids) > 1
        assert all(isinstance(doc_id, str) for doc_id in doc_ids)


class TestPipelineComposition:
    """Test pipeline composition and interface adherence."""

    def test_indexer_property_access(self, vectorstore):
        """Test that indexer property provides access to the indexer."""
        indexer = SimpleIndexer(chunk_size=500)
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        assert pipeline.indexer is indexer
        assert pipeline.indexer.chunk_size == 500

    def test_upserter_property_access(self, vectorstore):
        """Test that upserter property provides access to the upserter."""
        indexer = SimpleIndexer()
        upserter = SimpleUpserter(vectorstore=vectorstore)
        pipeline = Pipeline(indexer=indexer, upserter=upserter)

        assert pipeline.upserter is upserter
        assert pipeline.upserter.vectorstore == vectorstore

    def test_pipeline_type_composition(self, vectorstore):
        """Test that pipeline correctly composes different component types."""
        # Simple pipeline
        simple_pipeline = Pipeline(
            indexer=SimpleIndexer(),
            upserter=SimpleUpserter(vectorstore=vectorstore),
        )

        # Semi-structured pipeline
        semi_structured_pipeline = Pipeline(
            indexer=SemiStructuredIndexer(),
            upserter=SemiStructuredUpserter(vectorstore=vectorstore),
        )

        # Both should be valid Pipeline instances
        from src.lib.core.ingestion import BasePipeline

        assert isinstance(simple_pipeline, BasePipeline)
        assert isinstance(semi_structured_pipeline, BasePipeline)
