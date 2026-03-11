# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Fixtures for the generic pipeline e2e test suite.

All tests are parametrized over every registered pipeline type via the
``pipeline`` fixture.  Infrastructure fixtures (TEI, Qdrant, Postgres) are
inherited from the parent conftest.
"""

from __future__ import annotations

import uuid

import pytest
from langchain.indexes import SQLRecordManager
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore

from src.lib.core.ingestion.config import (
    PipelineConfig,
    PipelineResources,
    PipelineType,
    SemiStructuredIndexerConfig,
    SemiStructuredResources,
    SemiStructuredUpserterConfig,
    create_pipeline,
)
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.pipeline import BasePipeline


# ---------------------------------------------------------------------------
# Namespace / normalizer
# ---------------------------------------------------------------------------


@pytest.fixture
def namespace() -> str:
    """A fresh UUID hex string used as the namespace for each test."""
    return uuid.uuid4().hex


@pytest.fixture
def normalizer(namespace: str) -> MetadataFieldNormalizer:
    """MetadataFieldNormalizer that stamps ``namespace`` onto every document."""
    return MetadataFieldNormalizer(fields={"namespace": namespace})


# ---------------------------------------------------------------------------
# Parametrized pipeline — one run per algorithm type
# ---------------------------------------------------------------------------


@pytest.fixture(
    params=[PipelineType.SIMPLE, PipelineType.SEMI_STRUCTURED],
    ids=lambda t: t.value,
)
def pipeline(
    request: pytest.FixtureRequest,
    vectorstore: QdrantVectorStore,
    record_manager: SQLRecordManager,
    docstore_record_manager: SQLRecordManager,
    normalizer: MetadataFieldNormalizer,
) -> BasePipeline:
    """A fully wired pipeline, parametrized over all pipeline types.

    SIMPLE uses the vectorstore only.
    SEMI_STRUCTURED additionally receives the record_manager for deduplication.
    Both receive the namespace normalizer.
    """
    pipeline_type: PipelineType = request.param

    if pipeline_type == PipelineType.SIMPLE:
        config = PipelineConfig(pipeline_type=pipeline_type)
        resources = PipelineResources(
            vectorstore=vectorstore,
            normalizer=normalizer,
            record_manager=record_manager,
        )
    else:
        config = PipelineConfig(
            pipeline_type=pipeline_type,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(),
        )
        resources = SemiStructuredResources(
            vectorstore=vectorstore,
            record_manager=record_manager,
            docstore_record_manager=docstore_record_manager,
            normalizer=normalizer,
        )

    return create_pipeline(config=config, resources=resources)
