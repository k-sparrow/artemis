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
from enum import StrEnum
from typing import List, NamedTuple, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from langchain_core.documents import Document
from pydantic import BaseModel


__all__ = [
    # Protocols
    "PipelineInput",
    "ProcessedData",
    "PipelineOutput",
    # Concrete types
    "ChunkType",
    "ParsedChunk",
    "SplitChunks",
    "UpsertResult",
    # Type variables for generics
    "InputT",
    "ProcessedT",
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


class ChunkType(StrEnum):
    """Content type of a parsed document chunk.

    Values are aligned 1-to-1 with ``DocItemLabel`` from ``docling_core`` so
    that the full semantic signal from Docling is preserved through the HTTP
    boundary.  ``UNKNOWN`` is a catch-all for any label not yet modelled here,
    ensuring the parsing service never crashes on a future Docling update.
    """

    # Structural / layout
    TITLE = "title"
    SECTION_HEADER = "section_header"
    PAGE_HEADER = "page_header"
    PAGE_FOOTER = "page_footer"
    DOCUMENT_INDEX = "document_index"
    # Body content
    TEXT = "text"
    PARAGRAPH = "paragraph"
    TABLE = "table"
    PICTURE = "picture"
    CHART = "chart"
    CAPTION = "caption"
    FOOTNOTE = "footnote"
    LIST_ITEM = "list_item"
    CODE = "code"
    FORMULA = "formula"
    REFERENCE = "reference"
    # Form / interactive
    FORM = "form"
    KEY_VALUE_REGION = "key_value_region"
    CHECKBOX_SELECTED = "checkbox_selected"
    CHECKBOX_UNSELECTED = "checkbox_unselected"
    # Misc
    GRADING_SCALE = "grading_scale"
    HANDWRITTEN_TEXT = "handwritten_text"
    EMPTY_VALUE = "empty_value"
    # Fallback for any label not yet modelled above
    UNKNOWN = "unknown"


class ParsedChunk(BaseModel):
    """Schema contract between the parsing service and the indexing service.

    ``namespace`` is intentionally absent — it is stamped by the indexing
    service, not by the parser.  ``dl_meta`` and other loader-internal fields
    are dropped at this boundary.

    ``obj_id`` is the record-manager deduplication pivot, derived upstream as
    ``uuid5(namespace_id, source_label)`` by the storage service and stamped
    on every chunk by the parsing service so the indexing service can scope
    ``aindex`` cleanup to the correct object.
    """

    page_content: str
    source: str
    type: ChunkType
    obj_id: UUID


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
