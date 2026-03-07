# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Generic pipeline e2e tests.

Every test in this module is parametrized over all pipeline algorithm types
(SIMPLE, SEMI_STRUCTURED) via the ``pipeline`` fixture defined in conftest.py.

Only behaviour that must hold for *every* algorithm is tested here.
Algorithm-specific contracts (deduplication, cleanup modes, table separation)
belong in dedicated test modules.
"""

from __future__ import annotations

from typing import List

import pytest
from langchain.indexes import SQLRecordManager
from langchain_core.documents import Document
from qdrant_client import AsyncQdrantClient

from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore
from src.lib.core.ingestion.config import (
    PipelineConfig,
    PipelineResources,
    PipelineType,
    SemiStructuredIndexerConfig,
    SemiStructuredUpserterConfig,
    SimpleUpserterConfig,
    create_pipeline,
)
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.pipeline import BasePipeline


class TestGenericPipelineContract:
    """Contract tests that must hold for every pipeline algorithm type."""

    @pytest.mark.asyncio
    async def test_all_sources_produce_at_least_one_chunk(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
    ) -> None:
        """After ingestion, the vectorstore must hold at least one chunk per
        source document — nothing is silently dropped."""
        await pipeline.aprocess(sample_documents)

        result = await qdrant_client.count(vectorstore.collection_name)

        assert result.count >= len(sample_documents)

    @pytest.mark.asyncio
    async def test_all_chunks_carry_same_namespace(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        namespace: str,
    ) -> None:
        """Every stored chunk must carry the namespace injected by the
        normalizer — the field must survive chunking and storage intact."""
        await pipeline.aprocess(sample_documents)

        records, _ = await qdrant_client.scroll(
            collection_name=vectorstore.collection_name,
            with_payload=True,
            limit=1000,
        )

        assert len(records) > 0
        assert all(
            r.payload.get("metadata", {}).get("namespace") == namespace for r in records
        )

    @pytest.mark.asyncio
    async def test_source_metadata_preserved_on_chunks(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
    ) -> None:
        """The ``source`` key from each original document must appear on at
        least one stored chunk — metadata must not be stripped during chunking."""
        await pipeline.aprocess(sample_documents)

        records, _ = await qdrant_client.scroll(
            collection_name=vectorstore.collection_name,
            with_payload=True,
            limit=1000,
        )

        stored_sources = {r.payload.get("metadata", {}).get("source") for r in records}
        expected_sources = {doc.metadata["source"] for doc in sample_documents}

        assert expected_sources.issubset(stored_sources)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_type",
        [PipelineType.SIMPLE, PipelineType.SEMI_STRUCTURED],
        ids=lambda t: t.value,
    )
    async def test_full_cleanup_empties_vectorstore_and_record_manager(
        self,
        pipeline_type: PipelineType,
        vectorstore: QdrantVectorStore,
        record_manager: SQLRecordManager,
        normalizer: MetadataFieldNormalizer,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
    ) -> None:
        """Upserting an empty list with cleanup='full' must delete all previously
        ingested chunks from both the vectorstore and the record manager.

        Verified in three steps:
        1. Ingest documents — vectorstore is non-empty.
        2. Re-process with an empty list — vectorstore drops to zero.
        3. Re-ingest the same documents — record manager treats them as new
           (num_added > 0, num_skipped == 0), proving it was cleared too.
        """
        if pipeline_type == PipelineType.SIMPLE:
            config = PipelineConfig(
                pipeline_type=pipeline_type,
                upserter=SimpleUpserterConfig(cleanup="full"),
            )
        else:
            config = PipelineConfig(
                pipeline_type=pipeline_type,
                indexer=SemiStructuredIndexerConfig(),
                upserter=SemiStructuredUpserterConfig(cleanup="full"),
            )

        resources = PipelineResources(
            vectorstore=vectorstore,
            record_manager=record_manager,
            normalizer=normalizer,
        )
        pipeline = create_pipeline(config=config, resources=resources)

        # 1. Ingest — vectorstore must be non-empty
        await pipeline.aprocess(sample_documents)
        count_before = (await qdrant_client.count(vectorstore.collection_name)).count
        assert count_before > 0

        # 2. Empty upsert with full cleanup — vectorstore must be cleared
        await pipeline.aprocess([])
        count_after = (await qdrant_client.count(vectorstore.collection_name)).count
        assert count_after == 0

        # 3. Re-ingest — record manager must treat all docs as new again
        result = await pipeline.aprocess(sample_documents)
        assert result.num_added > 0
        assert result.num_skipped == 0
