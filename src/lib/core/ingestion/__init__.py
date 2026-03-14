"""Core ingestion pipeline for RAG document processing.

This module provides the building blocks for document ingestion pipelines:
- Pipeline: Orchestrates the flow from documents to vector store
- Indexer: Processes and chunks documents
- Upserter: Stores processed chunks in vector/document stores

Type Flow:
    Simple RAG:
        Sequence[Document] → Indexer → List[Document] → Upserter → UpsertResult

    Semi-Structured RAG:
        Sequence[Document] → Indexer → SplitChunks → Upserter → UpsertResult
"""

__all__ = [
    # Pipeline primitives
    "BasePipeline",
    "Pipeline",
    "Indexer",
    "Upserter",
    "SimpleIndexer",
    "SemiStructuredIndexer",
    "SimpleUpserter",
    "SemiStructuredUpserter",
    # Configuration & factory
    "PipelineType",
    "SimpleIndexerConfig",
    "SemiStructuredIndexerConfig",
    "SimpleUpserterConfig",
    "SemiStructuredUpserterConfig",
    "PipelineResources",
    "SemiStructuredResources",
    "PipelineConfig",
    "PIPELINE_TYPES",
    "create_pipeline",
    # Exceptions & type vars
    "DocumentProcessingException",
    "InputT",
    "ProcessedT",
    "SplitChunks",
    "UpsertResult",
]

from src.lib.core.ingestion.config import (
    PIPELINE_TYPES,
    PipelineConfig,
    PipelineResources,
    SemiStructuredResources,
    PipelineType,
    SemiStructuredIndexerConfig,
    SemiStructuredUpserterConfig,
    SimpleIndexerConfig,
    SimpleUpserterConfig,
    create_pipeline,
)
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.indexer import (
    Indexer,
    SemiStructuredIndexer,
    SimpleIndexer,
)
from src.lib.core.ingestion.pipeline import BasePipeline, Pipeline
from src.lib.core.ingestion.types import (
    InputT,
    ProcessedT,
    SplitChunks,
    UpsertResult,
)
from src.lib.core.ingestion.upserter import (
    SemiStructuredUpserter,
    SimpleUpserter,
    Upserter,
)
