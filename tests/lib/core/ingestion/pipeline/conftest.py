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
    SimpleUpserterConfig,
    create_pipeline,
)
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.pipeline import BasePipeline


# ---------------------------------------------------------------------------
# Namespace / normalizer
# ---------------------------------------------------------------------------


@pytest.fixture
def namespace() -> uuid.UUID:
    """A fresh UUID used as the namespace for each test."""
    return uuid.uuid4()


@pytest.fixture
def normalizer(namespace: uuid.UUID) -> MetadataFieldNormalizer:
    """MetadataFieldNormalizer that stamps ``namespace`` onto every document."""
    return MetadataFieldNormalizer(fields={"namespace": str(namespace)})


# ---------------------------------------------------------------------------
# source_id_key — parametrized over both supported values
# ---------------------------------------------------------------------------


@pytest.fixture(params=["source", "obj_id"])
def source_id_key(request: pytest.FixtureRequest) -> str:
    """The metadata field used as the record-manager deduplication key."""
    return request.param


# ---------------------------------------------------------------------------
# Parametrized pipeline — one run per (algorithm type × source_id_key)
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
    source_id_key: str,
) -> BasePipeline:
    """A fully wired pipeline, parametrized over all pipeline types and both
    supported source_id_key values (``source`` and ``obj_id``).
    """
    pipeline_type: PipelineType = request.param

    if pipeline_type == PipelineType.SIMPLE:
        config = PipelineConfig(
            pipeline_type=pipeline_type,
            upserter=SimpleUpserterConfig(source_id_key=source_id_key),
        )
        resources = PipelineResources(
            vectorstore=vectorstore,
            record_manager=record_manager,
        )
    else:
        config = PipelineConfig(
            pipeline_type=pipeline_type,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(source_id_key=source_id_key),
        )
        resources = SemiStructuredResources(
            vectorstore=vectorstore,
            record_manager=record_manager,
            docstore_record_manager=docstore_record_manager,
        )

    return create_pipeline(config=config, resources=resources)
