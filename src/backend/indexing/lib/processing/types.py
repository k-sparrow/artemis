"""Type definitions for the processing pipeline.

This module defines Protocol types for pipeline input, intermediate data,
and output. These protocols enable type-safe, composable pipelines while
allowing different concrete implementations.

Type Flow:
    Simple RAG:
        Sequence[Document] → Indexer → List[Document] → Upserter → List[str]

    Semi-Structured RAG:
        Sequence[Document] → Indexer → SplitChunks → Upserter → List[str]
"""

from typing import List, NamedTuple, Protocol, TypeVar, runtime_checkable

from langchain_core.documents import Document


__all__ = [
    # Protocols
    "PipelineInput",
    "ProcessedData",
    "PipelineOutput",
    # Concrete types
    "SplitChunks",
    # Type variables for generics
    "InputT",
    "ProcessedT",
    "OutputT",
]


@runtime_checkable
class PipelineInput(Protocol):
    """Protocol for pipeline input types.

    Any type that can be fed into a pipeline's process() method.
    This is intentionally minimal to allow flexibility in input shapes
    (e.g., Documents, file paths, raw bytes, URLs).
    """

    pass


@runtime_checkable
class ProcessedData(Protocol):
    """Protocol for intermediate processed data.

    Represents the output of an Indexer and input to an Upserter.
    This is the data that flows between pipeline stages after
    processing/chunking but before persistence.
    """

    pass


@runtime_checkable
class PipelineOutput(Protocol):
    """Protocol for pipeline output types.

    Any type that represents the result of pipeline processing.
    This could be document IDs, status objects, result summaries,
    or any other result representation.
    """

    pass


# -----------------------------------------------------------------------------
# Concrete Types
# -----------------------------------------------------------------------------


class SplitChunks(NamedTuple):
    """Intermediate type for semi-structured RAG pipelines.

    Separates text chunks from table chunks to optimize upserting.
    Both fields contain Document objects, but the separation prevents
    the upserter from having to re-classify or re-split a combined list.

    Attributes:
        text_chunks: Document chunks from text content
        table_chunks: Document chunks from tables/structured content
    """

    text_chunks: List[Document]
    table_chunks: List[Document]


# Type variables for generic base classes
# These allow concrete implementations to specify their exact types
# Note: Bounds are intentionally loose (Protocol with no methods) to allow
# any type while maintaining semantic meaning through the protocol names.

InputT = TypeVar("InputT")
"""Type variable for pipeline input."""

ProcessedT = TypeVar("ProcessedT")
"""Type variable for intermediate processed data."""

OutputT = TypeVar("OutputT")
"""Type variable for pipeline output."""
