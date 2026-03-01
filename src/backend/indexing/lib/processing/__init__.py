"""Legacy processing module - moved to src.lib.core.ingestion.

This module provides backward compatibility by re-exporting from the new location.
All new code should import directly from src.lib.core.ingestion.
"""

# Re-export from new location for backward compatibility
from src.lib.core.ingestion import (  # noqa: F401
    BasePipeline,
    Pipeline,
    Indexer,
    SimpleIndexer,
    SemiStructuredIndexer,
    Upserter,
    SimpleUpserter,
    SemiStructuredUpserter,
    InputT,
    OutputT,
    ProcessedT,
    SplitChunks,
    DocumentProcessingException,
)

__all__ = [
    # Pipeline
    "BasePipeline",
    "Pipeline",
    # Indexers
    "Indexer",
    "SimpleIndexer",
    "SemiStructuredIndexer",
    # Upserters
    "Upserter",
    "SimpleUpserter",
    "SemiStructuredUpserter",
    # Types
    "SplitChunks",
    "InputT",
    "ProcessedT",
    "OutputT",
    # Exceptions
    "DocumentProcessingException",
]
