# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Pipeline configuration and factory for the core ingestion library.

This module is the single place that knows how to map a *configuration*
(algorithm choice + hyperparameters) to a concrete :class:`Pipeline`
instance.  Callers — API services, Celery workers, experiment scripts —
only need to build a :class:`PipelineConfig` and hand it (together with a
:class:`PipelineResources` bundle) to :func:`create_pipeline`.

Example — experiment loop::

    from src.lib.core.ingestion.config import (
        PIPELINE_TYPES,
        PipelineConfig,
        PipelineResources,
        PipelineType,
        SemiStructuredIndexerConfig,
        SimpleIndexerConfig,
        create_pipeline,
    )

    resources = PipelineResources(vectorstore=vectorstore)
    for pipeline_type in PIPELINE_TYPES:
        for chunk_size in [256, 512, 1024]:
            if pipeline_type == PipelineType.SIMPLE:
                indexer = SimpleIndexerConfig(chunk_size=chunk_size)
            else:
                indexer = SemiStructuredIndexerConfig(chunk_size=chunk_size)
            config = PipelineConfig(pipeline_type=pipeline_type, indexer=indexer)
            pipeline = create_pipeline(config, resources)
            ids = await pipeline.aprocess(documents)
            # collect metrics …

Example — API dependency::

    resources = PipelineResources(vectorstore=vectorstore)
    config = PipelineConfig(pipeline_type=PipelineType(settings.PIPELINE_TYPE))
    pipeline = create_pipeline(config=config, resources=resources)
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import StrEnum
from typing import Any, Literal

from langchain.retrievers.multi_vector import SearchType
from langchain_core.indexing import RecordManager
from langchain_core.indexing.base import DocumentIndex
from langchain_core.retrievers import BaseRetriever
from langchain_core.runnables import Runnable
from langchain_core.stores import BaseStore
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.indexer import SimpleIndexer, SemiStructuredIndexer
from src.lib.core.ingestion.upserter import SimpleUpserter, SemiStructuredUpserter
from src.lib.core.ingestion.pipeline import BasePipeline, Pipeline
from src.lib.core.retrieval.adapters.semi_structured import (
    SemiStructuredRAGRetrievalAdapter,
)
from src.lib.core.retrieval.adapters.simple import SimpleVectorStoreRetrieverAdapter


__all__ = [
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
    "create_retriever",
]


class PipelineType(StrEnum):
    """Selects the indexer/upserter algorithm pairing."""

    SIMPLE = "simple"
    SEMI_STRUCTURED = "semi_structured"


# ---------------------------------------------------------------------------
# Indexer configs — one per algorithm, serializable (frozen dataclasses)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimpleIndexerConfig:
    """Hyperparameters for the simple flat-chunking indexer.

    Attributes:
        chunk_size: Maximum characters per text chunk.
        chunk_overlap: Characters of overlap between consecutive text chunks.
    """

    chunk_size: int = 1024
    chunk_overlap: int = 100


@dataclass(frozen=True)
class SemiStructuredIndexerConfig:
    """Hyperparameters for the semi-structured (text + table) indexer.

    Attributes:
        chunk_size: Maximum characters per text chunk.
        chunk_overlap: Overlap between consecutive text chunks.
        table_chunk_size: Maximum characters per table chunk.
            Only used when ``split_tables=True``.
        table_chunk_overlap: Overlap between consecutive table chunks.
            Only used when ``split_tables=True``.
        split_tables: When ``False`` (default), table documents pass through
            as single units — correct for multi-vector RAG where each table
            is summarized whole.  Set to ``True`` to chunk large tables
            when using the direct (no-docstore) mode.
    """

    chunk_size: int = 1024
    chunk_overlap: int = 100
    table_chunk_size: int = 2048
    table_chunk_overlap: int = 0
    split_tables: bool = False


# ---------------------------------------------------------------------------
# Upserter configs — one per algorithm, serializable (no runtime objects)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimpleUpserterConfig:
    """Configuration for the simple vectorstore upserter.

    The vectorstore and record_manager are runtime dependencies passed via
    :class:`PipelineResources`, not stored here.

    Attributes:
        cleanup: Deduplication strategy passed to ``aindex`` when a
            ``record_manager`` is provided.  Ignored otherwise.
        source_id_key: Document metadata key used to identify the source
            document for deduplication.
        batch_size: Number of documents per indexing batch.
    """

    cleanup: Literal["full", "incremental", "scoped_full", "none"] = "scoped_full"
    source_id_key: str = "source"
    batch_size: int = 32


@dataclass(frozen=True)
class SemiStructuredUpserterConfig:
    """Configuration for the semi-structured upserter.

    Controls LangChain ``aindex`` behaviour when a ``RecordManager`` is
    provided in :class:`PipelineResources`.

    Attributes:
        cleanup: Deduplication strategy passed to ``aindex``.
            ``"scoped_full"`` removes all old embeddings for sources present in
            the current batch (other sources are untouched) — the recommended
            default for incremental per-file ingestion.
            ``"incremental"`` removes old embeddings for changed sources only;
            ``"full"`` removes *all* embeddings not seen in the current run;
            ``"none"`` disables cleanup (equivalent to ``cleanup=None`` in
            LangChain's ``aindex``).
        source_id_key: Document metadata key used to identify the source
            document for deduplication.
    """

    cleanup: Literal["full", "incremental", "scoped_full", "none"] = "scoped_full"
    source_id_key: str = "source"
    batch_size: int = 32
    id_key: str = "doc_id"


# ---------------------------------------------------------------------------
# Runtime resources — not serializable, injected at construction time
# ---------------------------------------------------------------------------


@dataclass
class PipelineResources:
    """Runtime dependencies shared by all pipeline algorithms.

    These are live connection objects and are intentionally *not* part of
    the serialisable :class:`PipelineConfig`.  Only LangChain abstractions
    are referenced here — concrete implementations (Qdrant, SQLAlchemy, …)
    are an infrastructure concern and live in the FastAPI service layer and
    test conftest files.

    Attributes:
        vectorstore: Initialised vector store for embedding storage.
        record_manager: Optional record manager for deduplication via
            ``aindex``.
    """

    vectorstore: VectorStore
    record_manager: RecordManager | None = None


@dataclass
class SemiStructuredResources(PipelineResources):
    """Runtime dependencies for the semi-structured (multi-vector) pipeline.

    Extends :class:`PipelineResources` with the docstore and optional LLM
    summarizer chains needed for the multi-vector RAG pattern.

    Attributes:
        document_index: Docstore for the multi-vector RAG pattern.  When
            provided, original chunks are stored here keyed by UUID and their
            summaries are embedded in the vectorstore.
        docstore_record_manager: Optional record manager for the docstore.
            When provided alongside ``document_index``, originals are written
            via ``lc_aindex`` — giving the docstore the same deduplication and
            cleanup guarantees as the vectorstore.  Should use the same engine
            as ``record_manager`` but a distinct namespace (e.g. append
            ``":originals"``).
        table_summarizer: ``Runnable[str, str]`` that produces a concise text
            summary of a table chunk.  Required when ``document_index`` is
            provided — passed to :class:`SemiStructuredUpserter` which runs it
            after writing originals to the docstore.
        text_summarizer: Optional ``Runnable[str, str]`` for text chunks.
            When absent, text chunks are embedded as-is.
    """

    document_index: DocumentIndex | None = None
    docstore_record_manager: RecordManager | None = None
    table_summarizer: Runnable | None = None
    text_summarizer: Runnable | None = None


# ---------------------------------------------------------------------------
# Pipeline config — full, serializable specification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineConfig:
    """Full, serialisable specification of a pipeline.

    Combines a :class:`PipelineType` with the matching indexer and upserter
    config objects.  For each ``pipeline_type`` value there is a canonical
    config pair:

    - ``SIMPLE``          → :class:`SimpleIndexerConfig` + :class:`SimpleUpserterConfig`
    - ``SEMI_STRUCTURED`` → :class:`SemiStructuredIndexerConfig` + :class:`SemiStructuredUpserterConfig`  # noqa: E501

    Using a mismatched pair (e.g. ``SEMI_STRUCTURED`` with
    ``SimpleIndexerConfig``) is not an error; :func:`create_pipeline` will
    use type-appropriate defaults instead.

    Attributes:
        pipeline_type: Which indexer/upserter algorithm pairing to use.
        indexer: Algorithm-specific indexer hyperparameters.
        upserter: Algorithm-specific upserter configuration.
        metadata: Arbitrary extra key/value pairs (e.g. experiment labels).
    """

    pipeline_type: PipelineType = PipelineType.SIMPLE
    indexer: SimpleIndexerConfig | SemiStructuredIndexerConfig = field(
        default_factory=SimpleIndexerConfig
    )
    upserter: SimpleUpserterConfig | SemiStructuredUpserterConfig = field(
        default_factory=SimpleUpserterConfig
    )
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict suitable for JSON / env-var storage."""
        return {
            "pipeline_type": self.pipeline_type.value,
            "indexer": asdict(self.indexer),
            "upserter": asdict(self.upserter),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineConfig:
        """Deserialise from a plain dict produced by :meth:`to_dict`."""
        pt = PipelineType(data["pipeline_type"])
        match pt:
            case PipelineType.SIMPLE:
                indexer: SimpleIndexerConfig | SemiStructuredIndexerConfig = (
                    SimpleIndexerConfig(**data.get("indexer", {}))
                )
                upserter: SimpleUpserterConfig | SemiStructuredUpserterConfig = (
                    SimpleUpserterConfig(**data.get("upserter", {}))
                )
            case PipelineType.SEMI_STRUCTURED:
                indexer = SemiStructuredIndexerConfig(**data.get("indexer", {}))
                upserter = SemiStructuredUpserterConfig(**data.get("upserter", {}))
            case _:
                raise ValueError(f"Unknown pipeline type: {pt!r}")
        return cls(
            pipeline_type=pt,
            indexer=indexer,
            upserter=upserter,
            metadata=data.get("metadata", {}),
        )


#: All registered pipeline types — use for parametrised test discovery and
#: experiment iteration.
PIPELINE_TYPES: list[PipelineType] = list(PipelineType)


def create_pipeline(
    config: PipelineConfig, resources: PipelineResources
) -> BasePipeline:
    """Construct a :class:`Pipeline` from *config* and *resources*.

    Args:
        config: Full, serialisable pipeline specification.
        resources: Runtime dependencies (vectorstore, optional record manager).

    Returns:
        A ready-to-use :class:`BasePipeline` instance.

    Raises:
        ValueError: If *config.pipeline_type* is not a recognised value.
    """
    match config.pipeline_type:
        case PipelineType.SIMPLE:
            icfg = (
                config.indexer
                if isinstance(config.indexer, SimpleIndexerConfig)
                else SimpleIndexerConfig()
            )
            ucfg = (
                config.upserter
                if isinstance(config.upserter, SimpleUpserterConfig)
                else SimpleUpserterConfig()
            )
            return Pipeline(
                indexer=SimpleIndexer(
                    chunk_size=icfg.chunk_size,
                    chunk_overlap=icfg.chunk_overlap,
                ),
                upserter=SimpleUpserter(
                    vectorstore=resources.vectorstore,
                    record_manager=resources.record_manager,
                    cleanup=ucfg.cleanup,
                    source_id_key=ucfg.source_id_key,
                    batch_size=ucfg.batch_size,
                ),
            )
        case PipelineType.SEMI_STRUCTURED:
            icfg = (
                config.indexer
                if isinstance(config.indexer, SemiStructuredIndexerConfig)
                else SemiStructuredIndexerConfig()
            )
            ucfg = (
                config.upserter
                if isinstance(config.upserter, SemiStructuredUpserterConfig)
                else SemiStructuredUpserterConfig()
            )
            sr = resources if isinstance(resources, SemiStructuredResources) else None
            return Pipeline(
                indexer=SemiStructuredIndexer(
                    chunk_size=icfg.chunk_size,
                    chunk_overlap=icfg.chunk_overlap,
                    table_chunk_size=icfg.table_chunk_size,
                    table_chunk_overlap=icfg.table_chunk_overlap,
                    split_tables=icfg.split_tables,
                ),
                upserter=SemiStructuredUpserter(
                    vectorstore=resources.vectorstore,
                    document_index=sr.document_index if sr else None,
                    docstore_record_manager=sr.docstore_record_manager if sr else None,
                    record_manager=resources.record_manager,
                    id_key=ucfg.id_key,
                    cleanup=ucfg.cleanup,
                    source_id_key=ucfg.source_id_key,
                    batch_size=ucfg.batch_size,
                    table_summarizer=sr.table_summarizer if sr else None,
                    text_summarizer=sr.text_summarizer if sr else None,
                ),
            )
        case _:
            raise ValueError(f"Unknown pipeline type: {config.pipeline_type!r}")


def create_retriever(
    config: PipelineConfig,
    resources: PipelineResources,
    search_type: SearchType = SearchType.similarity,
    search_kwargs: dict[str, Any] | None = None,
) -> BaseRetriever:
    """Construct a retriever wired to the same config as the pipeline.

    Branches on ``config.pipeline_type``:

    - ``SIMPLE`` → :class:`SimpleVectorStoreRetrieverAdapter`
    - ``SEMI_STRUCTURED`` with a ``document_index`` →
      :class:`SemiStructuredRAGRetrievalAdapter` (multi-vector retrieval)
    - ``SEMI_STRUCTURED`` without a ``document_index`` →
      :class:`SimpleVectorStoreRetrieverAdapter` (direct mode)

    The ``id_key`` is read from ``config.upserter`` so it is guaranteed to
    match what :func:`create_pipeline` used during ingestion — this is the
    single source of truth that links summary embeddings back to originals.

    Args:
        config: The same :class:`PipelineConfig` used when building the pipeline.
        resources: Runtime dependencies.  Pass :class:`SemiStructuredResources`
            for multi-vector retrieval.
        search_type: Similarity search mode forwarded to the retriever.
        search_kwargs: Extra search kwargs, e.g. ``{"k": 4}``.

    Raises:
        ValueError: If *config.pipeline_type* is not a recognised value.
    """
    match config.pipeline_type:
        case PipelineType.SIMPLE:
            return SimpleVectorStoreRetrieverAdapter(
                vectorstore=resources.vectorstore,
            ).get_retriever(
                search_type=search_type,
                search_kwargs=search_kwargs or {},
            )
        case PipelineType.SEMI_STRUCTURED:
            sr = resources if isinstance(resources, SemiStructuredResources) else None
            if sr is None or sr.document_index is None:
                # Direct mode — no docstore, fall back to simple vector retrieval
                return SimpleVectorStoreRetrieverAdapter(
                    vectorstore=resources.vectorstore,
                ).get_retriever(
                    search_type=search_type,
                    search_kwargs=search_kwargs or {},
                )
            ucfg = (
                config.upserter
                if isinstance(config.upserter, SemiStructuredUpserterConfig)
                else SemiStructuredUpserterConfig()
            )
            # StoreDocumentIndex (and all DocumentIndex impls in this project)
            # expose the underlying ByteStore via .store.  We use getattr to
            # avoid importing the concrete adapter type here.
            byte_store: BaseStore = getattr(sr.document_index, "store")
            return SemiStructuredRAGRetrievalAdapter(
                vectorstore=resources.vectorstore,
                byte_store=byte_store,
                id_key=ucfg.id_key,
            ).get_retriever(
                search_type=search_type,
                search_kwargs=search_kwargs or {},
            )
        case _:
            raise ValueError(f"Unknown pipeline type: {config.pipeline_type!r}")
