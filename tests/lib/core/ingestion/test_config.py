# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Tests for src.lib.core.ingestion.config.

Unit tests — no Docker, no network, no vectorstore required.
All vectorstore interactions use a simple MagicMock.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.config import (
    PIPELINE_TYPES,
    PipelineConfig,
    PipelineResources,
    PipelineType,
    SemiStructuredIndexerConfig,
    SemiStructuredUpserterConfig,
    SimpleIndexerConfig,
    SimpleUpserterConfig,
    create_pipeline,
)
from src.lib.core.ingestion.indexer import SimpleIndexer, SemiStructuredIndexer
from src.lib.core.ingestion.pipeline import Pipeline
from src.lib.core.ingestion.upserter import SimpleUpserter, SemiStructuredUpserter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def vectorstore() -> VectorStore:
    return MagicMock(spec=VectorStore)


@pytest.fixture
def resources(vectorstore) -> PipelineResources:
    return PipelineResources(vectorstore=vectorstore)


# ---------------------------------------------------------------------------
# SimpleIndexerConfig
# ---------------------------------------------------------------------------


class TestSimpleIndexerConfig:
    def test_defaults(self):
        cfg = SimpleIndexerConfig()
        assert cfg.chunk_size == 1024
        assert cfg.chunk_overlap == 100

    def test_custom_values(self):
        cfg = SimpleIndexerConfig(chunk_size=512, chunk_overlap=50)
        assert cfg.chunk_size == 512
        assert cfg.chunk_overlap == 50

    def test_frozen(self):
        cfg = SimpleIndexerConfig()
        with pytest.raises(Exception):
            cfg.chunk_size = 9999  # type: ignore[misc]


# ---------------------------------------------------------------------------
# SemiStructuredIndexerConfig
# ---------------------------------------------------------------------------


class TestSemiStructuredIndexerConfig:
    def test_defaults(self):
        cfg = SemiStructuredIndexerConfig()
        assert cfg.chunk_size == 1024
        assert cfg.chunk_overlap == 100
        assert cfg.table_chunk_size == 2048
        assert cfg.table_chunk_overlap == 0

    def test_custom_table_params(self):
        cfg = SemiStructuredIndexerConfig(table_chunk_size=4096, table_chunk_overlap=64)
        assert cfg.table_chunk_size == 4096
        assert cfg.table_chunk_overlap == 64

    def test_frozen(self):
        cfg = SemiStructuredIndexerConfig()
        with pytest.raises(Exception):
            cfg.chunk_size = 9999  # type: ignore[misc]


# ---------------------------------------------------------------------------
# SimpleUpserterConfig
# ---------------------------------------------------------------------------


class TestSimpleUpserterConfig:
    def test_instantiates(self):
        cfg = SimpleUpserterConfig()
        assert cfg is not None

    def test_frozen(self):
        cfg = SimpleUpserterConfig()
        with pytest.raises(Exception):
            cfg.x = 1


# ---------------------------------------------------------------------------
# SemiStructuredUpserterConfig
# ---------------------------------------------------------------------------


class TestSemiStructuredUpserterConfig:
    def test_defaults(self):
        cfg = SemiStructuredUpserterConfig()
        assert cfg.cleanup == "scoped_full"
        assert cfg.source_id_key == "source"

    def test_cleanup_full(self):
        cfg = SemiStructuredUpserterConfig(cleanup="full")
        assert cfg.cleanup == "full"
    
    def test_cleanup_incremental(self):
        cfg = SemiStructuredUpserterConfig(cleanup="incremental")
        assert cfg.cleanup == "incremental"

    def test_cleanup_none_string(self):
        cfg = SemiStructuredUpserterConfig(cleanup="none")
        assert cfg.cleanup == "none"

    def test_custom_source_id_key(self):
        cfg = SemiStructuredUpserterConfig(source_id_key="doc_id")
        assert cfg.source_id_key == "doc_id"

    def test_frozen(self):
        cfg = SemiStructuredUpserterConfig()
        with pytest.raises(Exception):
            cfg.cleanup = "full"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# PipelineConfig
# ---------------------------------------------------------------------------


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.pipeline_type == PipelineType.SIMPLE
        assert isinstance(cfg.indexer, SimpleIndexerConfig)
        assert isinstance(cfg.upserter, SimpleUpserterConfig)
        assert cfg.metadata == {}

    def test_semi_structured_with_typed_configs(self):
        cfg = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(chunk_size=512),
            upserter=SemiStructuredUpserterConfig(cleanup="full"),
        )
        assert cfg.pipeline_type == PipelineType.SEMI_STRUCTURED
        assert isinstance(cfg.indexer, SemiStructuredIndexerConfig)
        assert cfg.indexer.chunk_size == 512
        assert isinstance(cfg.upserter, SemiStructuredUpserterConfig)
        assert cfg.upserter.cleanup == "full"

    def test_frozen(self):
        cfg = PipelineConfig()
        with pytest.raises(Exception):
            cfg.pipeline_type = PipelineType.SEMI_STRUCTURED  # type: ignore[misc]

    def test_to_dict_roundtrip_simple(self):
        original = PipelineConfig(
            pipeline_type=PipelineType.SIMPLE,
            indexer=SimpleIndexerConfig(chunk_size=256, chunk_overlap=32),
            metadata={"experiment": "run-1"},
        )
        restored = PipelineConfig.from_dict(original.to_dict())
        assert restored == original

    def test_to_dict_roundtrip_semi_structured(self):
        original = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(chunk_size=512, table_chunk_size=1024),
            upserter=SemiStructuredUpserterConfig(
                cleanup="full", source_id_key="doc_id"
            ),
        )
        restored = PipelineConfig.from_dict(original.to_dict())
        assert restored == original

    def test_to_dict_contains_string_type(self):
        cfg = PipelineConfig(pipeline_type=PipelineType.SIMPLE)
        d = cfg.to_dict()
        assert d["pipeline_type"] == "simple"
        assert isinstance(d["pipeline_type"], str)

    def test_from_dict_minimal_simple(self):
        cfg = PipelineConfig.from_dict({"pipeline_type": "simple"})
        assert cfg.pipeline_type == PipelineType.SIMPLE
        assert isinstance(cfg.indexer, SimpleIndexerConfig)
        assert isinstance(cfg.upserter, SimpleUpserterConfig)

    def test_from_dict_minimal_semi_structured(self):
        cfg = PipelineConfig.from_dict({"pipeline_type": "semi_structured"})
        assert cfg.pipeline_type == PipelineType.SEMI_STRUCTURED
        assert isinstance(cfg.indexer, SemiStructuredIndexerConfig)
        assert isinstance(cfg.upserter, SemiStructuredUpserterConfig)

    def test_from_dict_unknown_type_raises(self):
        with pytest.raises(ValueError):
            PipelineConfig.from_dict({"pipeline_type": "nonexistent"})


# ---------------------------------------------------------------------------
# PIPELINE_TYPES
# ---------------------------------------------------------------------------


class TestPipelineTypes:
    def test_contains_all_enum_members(self):
        assert set(PIPELINE_TYPES) == set(PipelineType)

    def test_is_list(self):
        assert isinstance(PIPELINE_TYPES, list)

    def test_non_empty(self):
        assert len(PIPELINE_TYPES) >= 2


# ---------------------------------------------------------------------------
# create_pipeline
# ---------------------------------------------------------------------------


class TestCreatePipeline:
    def test_simple_returns_pipeline(self, resources):
        cfg = PipelineConfig(pipeline_type=PipelineType.SIMPLE)
        pipeline = create_pipeline(cfg, resources)
        assert isinstance(pipeline, Pipeline)

    def test_semi_structured_returns_pipeline(self, resources):
        cfg = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(),
        )
        pipeline = create_pipeline(cfg, resources)
        assert isinstance(pipeline, Pipeline)

    def test_simple_uses_simple_indexer(self, resources):
        cfg = PipelineConfig(pipeline_type=PipelineType.SIMPLE)
        pipeline = create_pipeline(cfg, resources)
        assert isinstance(pipeline.indexer, SimpleIndexer)

    def test_semi_structured_uses_semi_structured_indexer(self, resources):
        cfg = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(),
        )
        pipeline = create_pipeline(cfg, resources)
        assert isinstance(pipeline.indexer, SemiStructuredIndexer)

    def test_simple_uses_simple_upserter(self, resources):
        cfg = PipelineConfig(pipeline_type=PipelineType.SIMPLE)
        pipeline = create_pipeline(cfg, resources)
        assert isinstance(pipeline.upserter, SimpleUpserter)

    def test_semi_structured_uses_semi_structured_upserter(self, resources):
        cfg = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(),
        )
        pipeline = create_pipeline(cfg, resources)
        assert isinstance(pipeline.upserter, SemiStructuredUpserter)

    def test_simple_indexer_config_propagates(self, resources):
        cfg = PipelineConfig(
            pipeline_type=PipelineType.SIMPLE,
            indexer=SimpleIndexerConfig(chunk_size=512, chunk_overlap=32),
        )
        pipeline = create_pipeline(cfg, resources)
        assert pipeline.indexer.chunk_size == 512
        assert pipeline.indexer.chunk_overlap == 32

    def test_semi_structured_indexer_config_propagates(self, resources):
        cfg = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(
                table_chunk_size=4096, table_chunk_overlap=64
            ),
            upserter=SemiStructuredUpserterConfig(),
        )
        pipeline = create_pipeline(cfg, resources)
        assert pipeline.indexer.table_chunk_size == 4096
        assert pipeline.indexer.table_chunk_overlap == 64


# ---------------------------------------------------------------------------
# Parametrised: every pipeline type must be constructible
# ---------------------------------------------------------------------------


def _default_config(pipeline_type: PipelineType) -> PipelineConfig:
    """Return a fully-typed PipelineConfig with algorithm-appropriate defaults."""
    if pipeline_type == PipelineType.SIMPLE:
        return PipelineConfig(pipeline_type=pipeline_type)
    return PipelineConfig(
        pipeline_type=pipeline_type,
        indexer=SemiStructuredIndexerConfig(),
        upserter=SemiStructuredUpserterConfig(),
    )


@pytest.mark.parametrize("pipeline_type", PIPELINE_TYPES)
class TestAllPipelineTypes:
    def test_create_succeeds(self, pipeline_type, resources):
        cfg = _default_config(pipeline_type)
        pipeline = create_pipeline(cfg, resources)
        assert pipeline is not None

    def test_indexer_exposes_chunk_size(self, pipeline_type, resources):
        cfg = _default_config(pipeline_type)
        pipeline = create_pipeline(cfg, resources)
        assert pipeline.indexer.chunk_size is not None

    def test_config_roundtrip_preserves_type(self, pipeline_type, resources):
        cfg = _default_config(pipeline_type)
        restored = PipelineConfig.from_dict(cfg.to_dict())
        pipeline = create_pipeline(restored, resources)
        assert isinstance(pipeline, Pipeline)
