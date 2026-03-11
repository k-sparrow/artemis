"""Type definitions for the processing pipeline.

This module defines Protocol types for pipeline input, intermediate data,
and output. These protocols enable type-safe, composable pipelines while
allowing different concrete implementations.

Type Flow:
    Simple RAG:
        Sequence[Document] → Indexer → List[Document] → Upserter → UpsertResult

    Semi-Structured RAG:
        Sequence[Document] → Indexer → SplitChunks → Upserter → UpsertResult
"""

from dataclasses import dataclass, field
from typing import List, NamedTuple, Protocol, TypeVar, runtime_checkable

from langchain_core.documents import Document


__all__ = [
    # Protocols
    "PipelineInput",
    "ProcessedData",
    "PipelineOutput",
    # Concrete types
    "SplitChunks",
    "UpsertResult",
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


@dataclass
class UpsertResult:
    """Result of an upserter operation.

    Combines the LangChain ``IndexingResult`` counts with the actual
    vectorstore IDs of documents that were added in this run.  IDs are
    captured via :class:`_IDCapturingVectorStore` because LangChain's
    ``index``/``aindex`` helpers do not surface them in their return value.

    Attributes:
        num_added: Documents newly written to the vectorstore.
        num_updated: Documents whose content changed and were re-written.
        num_skipped: Documents already present and unchanged (no write).
        num_deleted: Documents removed during cleanup.
        ids: Vectorstore IDs of the documents written in this run
            (``num_added + num_updated`` entries).
    """

    num_added: int = 0
    num_updated: int = 0
    num_skipped: int = 0
    num_deleted: int = 0
    ids: List[str] = field(default_factory=list)


class SplitChunks(NamedTuple):
    """Intermediate type for semi-structured RAG pipelines.

    Separates text chunks from table chunks so the upserter can apply
    different summarization strategies per content type without re-classifying
    a combined list.

    Attributes:
        text_chunks: Text chunks produced by the indexer.
        table_chunks: Table chunks produced by the indexer.
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
