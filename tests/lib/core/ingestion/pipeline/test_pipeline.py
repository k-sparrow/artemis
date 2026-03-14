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

import uuid
from typing import List

import pytest
from langchain.indexes import SQLRecordManager
from langchain_core.documents import Document
from langchain_core.runnables import RunnableLambda
from qdrant_client import AsyncQdrantClient

from src.lib.core.adapters.stores.sql.store import SQLDocumentIndex
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

# Identity summarizer: activates multi-vector mode without a real LLM.
# Returns each chunk's content unchanged so the summary == the original.
_IDENTITY_SUMMARIZER = RunnableLambda(lambda text: text)


class TestGenericPipelineContract:
    """Contract tests that must hold for every pipeline algorithm type."""

    @pytest.mark.asyncio
    async def test_all_sources_produce_at_least_one_chunk(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        normalizer: MetadataFieldNormalizer,
    ) -> None:
        """After ingestion, the vectorstore must hold at least one chunk per
        source document — nothing is silently dropped."""
        docs = await normalizer.anormalize(sample_documents)
        await pipeline.aprocess(docs)

        result = await qdrant_client.count(vectorstore.collection_name)

        assert result.count >= len(sample_documents)

    @pytest.mark.asyncio
    async def test_all_chunks_carry_same_namespace(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        normalizer: MetadataFieldNormalizer,
        namespace: uuid.UUID,
    ) -> None:
        """Every stored chunk must carry the namespace stamped by the service
        layer — the field must survive chunking and storage intact."""
        docs = await normalizer.anormalize(sample_documents)
        await pipeline.aprocess(docs)

        records, _ = await qdrant_client.scroll(
            collection_name=vectorstore.collection_name,
            with_payload=True,
            limit=1000,
        )

        assert len(records) > 0
        assert all(
            r.payload.get("metadata", {}).get("namespace") == str(namespace)
            for r in records
        )

    @pytest.mark.asyncio
    async def test_source_metadata_preserved_on_chunks(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        normalizer: MetadataFieldNormalizer,
    ) -> None:
        """The ``source`` key from each original document must appear on at
        least one stored chunk — metadata must not be stripped during chunking."""
        docs = await normalizer.anormalize(sample_documents)
        await pipeline.aprocess(docs)

        records, _ = await qdrant_client.scroll(
            collection_name=vectorstore.collection_name,
            with_payload=True,
            limit=1000,
        )

        stored_sources = {r.payload.get("metadata", {}).get("source") for r in records}
        expected_sources = {doc.metadata["source"] for doc in sample_documents}

        assert expected_sources.issubset(stored_sources)

    @pytest.mark.asyncio
    async def test_input_metadata_preserved_as_subset_on_chunks(
        self,
        pipeline: BasePipeline,
        vectorstore: QdrantVectorStore,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        normalizer: MetadataFieldNormalizer,
    ) -> None:
        """Every stored chunk must contain all metadata keys from its parent
        document — the pipeline must not drop any field during chunking or storage."""
        docs = await normalizer.anormalize(sample_documents)
        # Union of keys per source: multiple parent docs may share a source
        # (e.g. different pages of the same file) so we collect all keys together.
        parent_keys_by_source: dict[str, set[str]] = {}
        for doc in docs:
            source = doc.metadata["source"]
            parent_keys_by_source.setdefault(source, set()).update(doc.metadata.keys())
        await pipeline.aprocess(docs)

        records, _ = await qdrant_client.scroll(
            collection_name=vectorstore.collection_name,
            with_payload=True,
            limit=1000,
        )

        assert len(records) > 0
        for r in records:
            chunk_meta = r.payload.get("metadata", {})
            source = chunk_meta.get("source")
            assert source in parent_keys_by_source.keys(), (
                f"Pipeline mutated one of the chunks, '{source}' does not exist "
                f"in the original set of documents: [{list(parent_keys_by_source.keys())}]"
            )
            for key in parent_keys_by_source[source]:
                assert key in chunk_meta, (
                    f"metadata field '{key}' from parent doc '{source}' "
                    f"was lost in stored chunk"
                )

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
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        normalizer: MetadataFieldNormalizer,
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
            resources: PipelineResources = PipelineResources(
                vectorstore=vectorstore,
                record_manager=record_manager,
            )
        else:
            config = PipelineConfig(
                pipeline_type=pipeline_type,
                indexer=SemiStructuredIndexerConfig(),
                upserter=SemiStructuredUpserterConfig(cleanup="full"),
            )
            resources = SemiStructuredResources(
                vectorstore=vectorstore,
                record_manager=record_manager,
            )
        pipeline = create_pipeline(config=config, resources=resources)

        docs = await normalizer.anormalize(sample_documents)

        # 1. Ingest — vectorstore must be non-empty
        await pipeline.aprocess(docs)
        count_before = (await qdrant_client.count(vectorstore.collection_name)).count
        assert count_before > 0

        # 2. Empty upsert with full cleanup — vectorstore must be cleared
        await pipeline.aprocess([])
        count_after = (await qdrant_client.count(vectorstore.collection_name)).count
        assert count_after == 0

        # 3. Re-ingest — record manager must treat all docs as new again
        result = await pipeline.aprocess(docs)
        assert result.num_added > 0
        assert result.num_skipped == 0

    @pytest.mark.asyncio
    async def test_full_cleanup_empties_docstore(
        self,
        vectorstore: QdrantVectorStore,
        record_manager: SQLRecordManager,
        docstore_record_manager: SQLRecordManager,
        document_index: SQLDocumentIndex,
        qdrant_client: AsyncQdrantClient,
        sample_documents: List[Document],
        normalizer: MetadataFieldNormalizer,
    ) -> None:
        """Full cleanup must remove originals from the docstore, not just the
        vectorstore.

        Uses an identity summarizer to activate multi-vector mode without
        a real LLM — summaries are identical to originals, which is fine
        for testing the storage/cleanup path.

        Verified in three steps:
        1. Ingest — docstore contains originals for every vectorstore point.
        2. Empty aprocess with cleanup='full' — docstore entries are deleted.
        3. Direct aget confirms every doc_id now returns None.
        """
        config = PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(cleanup="full"),
        )
        resources = SemiStructuredResources(
            vectorstore=vectorstore,
            record_manager=record_manager,
            docstore_record_manager=docstore_record_manager,
            document_index=document_index,
            table_summarizer=_IDENTITY_SUMMARIZER,
        )
        pipeline = create_pipeline(config=config, resources=resources)

        docs = await normalizer.anormalize(sample_documents)

        # 1. Ingest — docstore must hold originals for every vectorstore point
        await pipeline.aprocess(docs)

        points, _ = await qdrant_client.scroll(
            collection_name=vectorstore.collection_name,
            with_payload=True,
            limit=1000,
        )
        doc_ids = [
            p.payload.get("metadata", {}).get("doc_id")
            for p in points
            if p.payload.get("metadata", {}).get("doc_id")
        ]
        assert len(doc_ids) > 0, "expected doc_ids in vectorstore payload"

        originals = await document_index.aget(doc_ids)
        assert all(
            orig is not None for orig in originals
        ), "all originals should be in docstore after ingestion"

        # 2. Full cleanup with empty list — docstore must be cleared
        await pipeline.aprocess([])

        # 3. Every doc_id must now return None from the docstore
        originals_after = await document_index.aget(doc_ids)
        assert all(
            orig is None for orig in originals_after
        ), "all originals should be removed from docstore after full cleanup"
