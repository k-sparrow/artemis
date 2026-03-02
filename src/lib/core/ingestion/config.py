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

from langchain_core.indexing import RecordManager
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.indexer import SimpleIndexer, SemiStructuredIndexer
from src.lib.core.ingestion.normalizer import DocumentNormalizer
from src.lib.core.ingestion.upserter import SimpleUpserter, SemiStructuredUpserter
from src.lib.core.ingestion.pipeline import BasePipeline, Pipeline


__all__ = [
    "PipelineType",
    "SimpleIndexerConfig",
    "SemiStructuredIndexerConfig",
    "SimpleUpserterConfig",
    "SemiStructuredUpserterConfig",
    "PipelineResources",
    "PipelineConfig",
    "PIPELINE_TYPES",
    "create_pipeline",
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
        table_chunk_overlap: Overlap between consecutive table chunks.
    """

    chunk_size: int = 1024
    chunk_overlap: int = 100
    table_chunk_size: int = 2048
    table_chunk_overlap: int = 0


# ---------------------------------------------------------------------------
# Upserter configs — one per algorithm, serializable (no runtime objects)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SimpleUpserterConfig:
    """Configuration for the simple vectorstore upserter.

    The vectorstore itself is a runtime dependency passed via
    :class:`PipelineResources`, not stored here.
    """


@dataclass(frozen=True)
class SemiStructuredUpserterConfig:
    """Configuration for the semi-structured upserter.

    Controls LangChain ``aindex`` behaviour when a ``RecordManager`` is
    provided in :class:`PipelineResources`.

    Attributes:
        cleanup: Deduplication strategy passed to ``aindex``.
            ``"incremental"`` removes old embeddings for changed sources;
            ``"full"`` removes *all* embeddings not seen in the current run;
            ``"none"`` disables cleanup (equivalent to ``cleanup=None`` in
            LangChain's ``aindex``).
        source_id_key: Document metadata key used to identify the source
            document for deduplication.
    """

    cleanup: Literal["full", "incremental", "none"] = "incremental"
    source_id_key: str = "source"
    batch_size: int = 32


# ---------------------------------------------------------------------------
# Runtime resources — not serializable, injected at construction time
# ---------------------------------------------------------------------------


@dataclass
class PipelineResources:
    """Runtime dependencies for a pipeline.

    These are live connection objects and are intentionally *not* part of
    the serialisable :class:`PipelineConfig`.

    Attributes:
        vectorstore: Initialised vector store for embedding storage.
        record_manager: Optional record manager for deduplication via
            ``aindex``.  Required when using
            :class:`SemiStructuredUpserterConfig` with cleanup enabled.
    """

    vectorstore: VectorStore
    record_manager: RecordManager | None = None
    normalizer: DocumentNormalizer | None = None


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
            return Pipeline(
                indexer=SimpleIndexer(
                    chunk_size=icfg.chunk_size,
                    chunk_overlap=icfg.chunk_overlap,
                ),
                upserter=SimpleUpserter(vectorstore=resources.vectorstore),
                normalizer=resources.normalizer,
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
            return Pipeline(
                indexer=SemiStructuredIndexer(
                    chunk_size=icfg.chunk_size,
                    chunk_overlap=icfg.chunk_overlap,
                    table_chunk_size=icfg.table_chunk_size,
                    table_chunk_overlap=icfg.table_chunk_overlap,
                ),
                upserter=SemiStructuredUpserter(
                    vectorstore=resources.vectorstore,
                    record_manager=resources.record_manager,
                    cleanup=ucfg.cleanup,
                    source_id_key=ucfg.source_id_key,
                    batch_size=ucfg.batch_size,
                ),
                normalizer=resources.normalizer,
            )
        case _:
            raise ValueError(f"Unknown pipeline type: {config.pipeline_type!r}")
