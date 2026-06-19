# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the ParentPagePipeline decorator (no containers).

The pipeline is exercised over an in-memory record manager + byte store and a
``RecordingInner`` standing in for ANY inner ``BasePipeline`` (Simple,
SemiStructured, future GraphRAG). The inner is never told about pages — that is
the whole point of the decorator, asserted by ``TestComposability``.
"""

from __future__ import annotations

from typing import List

import pytest
from langchain_core.documents import Document
from langchain_core.embeddings import DeterministicFakeEmbedding
from langchain_core.indexing import InMemoryRecordManager
from langchain_core.stores import InMemoryByteStore
from langchain_core.vectorstores import InMemoryVectorStore

from src.lib.core.adapters.stores.base.store import (
    MetadataKeyedDocumentIndex,
    StoreDocumentIndex,
)
from src.lib.core.ingestion.config import (
    PipelineConfig,
    PipelineType,
    SemiStructuredIndexerConfig,
    SemiStructuredResources,
    SemiStructuredUpserterConfig,
    create_pipeline,
)
from src.lib.core.ingestion.pipeline.base import BasePipeline
from src.lib.core.ingestion.pipeline.parent_page import (
    PAGE_KEY_FIELD,
    PARENT_ID_FIELD,
    PagedInput,
    ParentPagePipeline,
    page_key,
)
from src.lib.core.ingestion.types import UpsertResult
from src.lib.core.retrieval.parent_page import ParentPageRetriever

NS = "ns1"


class RecordingInner(BasePipeline):
    """A fake inner pipeline that records what it is asked to do.

    Stands in for any algorithm-specific pipeline; it only ever expects the
    plain chunk Documents the decorator delegates to it.
    """

    def __init__(self) -> None:
        self.aprocess_calls: List[list] = []
        self.adelete_calls: List[str] = []

    async def aprocess(self, data: list) -> UpsertResult:
        self.aprocess_calls.append(data)
        return UpsertResult(num_added=len(data))

    async def adelete_source(self, source: str) -> None:
        self.adelete_calls.append(source)

    def process(self, data: list) -> UpsertResult:
        raise NotImplementedError

    def delete_source(self, source: str) -> None:
        raise NotImplementedError


async def _make_pipeline(inner: BasePipeline, namespace_id: str = NS):
    store = InMemoryByteStore()
    record_manager = InMemoryRecordManager(namespace=f"{namespace_id}:pages")
    await record_manager.acreate_schema()
    pipeline = ParentPagePipeline(
        inner,
        record_manager=record_manager,
        document_index=MetadataKeyedDocumentIndex(store=store),
        byte_store=store,
        namespace_id=namespace_id,
    )
    return pipeline, store, record_manager


def _page(obj: str, page_no: int) -> Document:
    return Document(
        page_content=f"{obj} page {page_no} markdown",
        metadata={"obj_id": obj, "page_no": page_no, "namespace_id": NS},
    )


def _chunk(obj: str, page_no, text: str = "chunk") -> Document:
    return Document(
        page_content=text,
        metadata={
            "obj_id": obj,
            "page_no": page_no,
            "namespace_id": NS,
            "source": "a.pdf",
            "type": "text",
        },
    )


async def _keys(store: InMemoryByteStore) -> List[str]:
    return sorted([k async for k in store.ayield_keys()])


class TestIndexing:
    @pytest.mark.asyncio
    async def test_pages_written_under_deterministic_keys(self) -> None:
        inner = RecordingInner()
        pipeline, store, _ = await _make_pipeline(inner)

        await pipeline.aprocess(
            PagedInput(pages=[_page("objA", 1), _page("objA", 2)], chunks=[])
        )

        assert await _keys(store) == ["ns1/objA/p1", "ns1/objA/p2"]

    @pytest.mark.asyncio
    async def test_chunk_gets_parent_id_for_its_page(self) -> None:
        inner = RecordingInner()
        pipeline, _, _ = await _make_pipeline(inner)

        chunk = _chunk("objA", 2)
        await pipeline.aprocess(
            PagedInput(pages=[_page("objA", 1), _page("objA", 2)], chunks=[chunk])
        )

        assert chunk.metadata[PARENT_ID_FIELD] == page_key(NS, "objA", 2)

    @pytest.mark.asyncio
    async def test_chunk_with_no_page_falls_back_to_first_page(self) -> None:
        inner = RecordingInner()
        pipeline, _, _ = await _make_pipeline(inner)

        no_page = _chunk("objA", None)
        bad_page = _chunk("objA", 99)  # page that does not exist
        await pipeline.aprocess(
            PagedInput(
                pages=[_page("objA", 2), _page("objA", 3)],  # first == p2
                chunks=[no_page, bad_page],
            )
        )

        assert no_page.metadata[PARENT_ID_FIELD] == page_key(NS, "objA", 2)
        assert bad_page.metadata[PARENT_ID_FIELD] == page_key(NS, "objA", 2)

    @pytest.mark.asyncio
    async def test_chunk_for_object_without_pages_is_unlinked(self) -> None:
        inner = RecordingInner()
        pipeline, _, _ = await _make_pipeline(inner)

        orphan = _chunk("objB", 1)  # objB has no pages
        await pipeline.aprocess(PagedInput(pages=[_page("objA", 1)], chunks=[orphan]))

        assert PARENT_ID_FIELD not in orphan.metadata

    @pytest.mark.asyncio
    async def test_shrinking_page_count_reconciles_store(self) -> None:
        inner = RecordingInner()
        pipeline, store, _ = await _make_pipeline(inner)

        await pipeline.aprocess(
            PagedInput(
                pages=[_page("objA", 1), _page("objA", 2), _page("objA", 3)],
                chunks=[],
            )
        )
        # Re-ingest with one fewer page — the stale key must be reconciled away
        # (lc_index's uid cleanup can't reach the deterministic key, §9).
        await pipeline.aprocess(
            PagedInput(pages=[_page("objA", 1), _page("objA", 2)], chunks=[])
        )

        assert await _keys(store) == ["ns1/objA/p1", "ns1/objA/p2"]


class TestDeletion:
    @pytest.mark.asyncio
    async def test_delete_source_drops_pages_and_delegates(self) -> None:
        inner = RecordingInner()
        pipeline, store, record_manager = await _make_pipeline(inner)

        await pipeline.aprocess(
            PagedInput(
                pages=[_page("objA", 1), _page("objB", 1)],
                chunks=[_chunk("objA", 1)],
            )
        )
        assert await record_manager.alist_keys(group_ids=["objA"])  # recorded

        await pipeline.adelete_source("objA")

        # objA's page gone; objB's page untouched.
        assert await _keys(store) == ["ns1/objB/p1"]
        # Page record-manager rows for objA cleared.
        assert await record_manager.alist_keys(group_ids=["objA"]) == []
        # Inner (algorithm) deletion delegated.
        assert inner.adelete_calls == ["objA"]

    @pytest.mark.asyncio
    async def test_namespace_wipe_clears_prefix_and_delegates_empty(self) -> None:
        inner = RecordingInner()
        pipeline, store, _ = await _make_pipeline(inner)

        await pipeline.aprocess(
            PagedInput(pages=[_page("objA", 1), _page("objB", 1)], chunks=[])
        )

        await pipeline.aprocess(PagedInput())  # empty == namespace wipe

        assert await _keys(store) == []
        # Inner received an empty list (its own unscoped full cleanup).
        assert inner.aprocess_calls[-1] == []


class TestComposability:
    """The inner algorithm pipeline must stay completely unaware of pages."""

    @pytest.mark.asyncio
    async def test_inner_receives_only_chunks_never_pages(self) -> None:
        inner = RecordingInner()
        pipeline, _, _ = await _make_pipeline(inner)

        chunks = [_chunk("objA", 1), _chunk("objA", 2)]
        await pipeline.aprocess(
            PagedInput(pages=[_page("objA", 1), _page("objA", 2)], chunks=chunks)
        )

        # The exact chunk list is delegated — no PagedInput, no Page documents.
        assert inner.aprocess_calls == [chunks]
        for received in inner.aprocess_calls[0]:
            assert "page_key" not in received.metadata  # pages carry page_key

    @pytest.mark.asyncio
    async def test_idempotent_reingest_still_delegates_chunks(self) -> None:
        inner = RecordingInner()
        pipeline, store, _ = await _make_pipeline(inner)

        payload = lambda: PagedInput(  # noqa: E731
            pages=[_page("objA", 1)], chunks=[_chunk("objA", 1)]
        )
        await pipeline.aprocess(payload())
        await pipeline.aprocess(payload())

        # Store is stable; both runs delegated their chunks to the inner.
        assert await _keys(store) == ["ns1/objA/p1"]
        assert len(inner.aprocess_calls) == 2


def test_page_key_is_single_source_of_truth() -> None:
    # The same function produces a page's store key and a chunk's parent_id.
    assert page_key("ns", "obj", 3) == "ns/obj/p3"


# ---------------------------------------------------------------------------
# Composition over a REAL SemiStructured inner (no containers, no LLM)
# ---------------------------------------------------------------------------


async def _make_semi_structured_pipeline(namespace_id: str = NS):
    """Wrap a REAL multi-vector ``SemiStructured`` pipeline in the decorator.

    Built via the production ``create_pipeline`` factory with in-memory backends
    and ``None`` summarizers — the upserter then embeds each original verbatim
    (``summary_texts = texts``), so the full original→docstore + summary→vectorstore
    multi-vector cycle runs with **no LLM**.  This is what closes the gap the
    ``RecordingInner`` tests cannot: it proves ``parent_id`` (stamped upstream on
    the chunks) survives that real transform onto both the embedded summaries
    (the service's Simple-over-summaries retrieval path) and the docstore
    originals (the latent MultiVectorRetriever path), where it stays DISTINCT from
    SemiStructured's own ``doc_id`` pointer.
    """
    vectorstore = InMemoryVectorStore(DeterministicFakeEmbedding(size=16))
    vs_rm = InMemoryRecordManager(namespace=f"qdrant/{namespace_id}")
    await vs_rm.acreate_schema()

    document_index = StoreDocumentIndex(store=InMemoryByteStore())
    docstore_rm = InMemoryRecordManager(namespace=f"docstore/{namespace_id}:originals")
    await docstore_rm.acreate_schema()

    inner = create_pipeline(
        config=PipelineConfig(
            pipeline_type=PipelineType.SEMI_STRUCTURED,
            indexer=SemiStructuredIndexerConfig(),
            upserter=SemiStructuredUpserterConfig(),
        ),
        resources=SemiStructuredResources(
            vectorstore=vectorstore,
            record_manager=vs_rm,
            document_index=document_index,
            docstore_record_manager=docstore_rm,
            table_summarizer=None,  # identity passthrough — no LLM needed
            text_summarizer=None,
        ),
    )

    page_store = InMemoryByteStore()
    page_rm = InMemoryRecordManager(namespace=f"{namespace_id}:pages")
    await page_rm.acreate_schema()
    pipeline = ParentPagePipeline(
        inner,
        record_manager=page_rm,
        document_index=MetadataKeyedDocumentIndex(store=page_store),
        byte_store=page_store,
        namespace_id=namespace_id,
    )
    return pipeline, vectorstore, document_index, docstore_rm, page_store


def _distinct_chunks(obj: str) -> List[Document]:
    # Distinct content so originals/summaries don't collapse; 2 on p1, 1 on p2.
    return [
        _chunk(obj, 1, "alpha revenue grew"),
        _chunk(obj, 1, "beta revenue emea"),
        _chunk(obj, 2, "gamma costs automation"),
    ]


class TestSemiStructuredComposition:
    """The decorator composes over the real multi-vector algorithm: ``parent_id``
    rides through the original→summary transform, untouched by SemiStructured."""

    @pytest.mark.asyncio
    async def test_parent_id_rides_onto_embedded_summaries(self) -> None:
        pipeline, vectorstore, _, _, _ = await _make_semi_structured_pipeline()
        pages = [_page("objA", 1), _page("objA", 2)]
        await pipeline.aprocess(
            PagedInput(pages=pages, chunks=_distinct_chunks("objA"))
        )

        # The service retrieves SemiStructured via Simple search over the
        # vectorstore — i.e. over the SUMMARIES. Each must carry parent_id.
        summaries = await vectorstore.asimilarity_search("revenue costs", k=10)
        assert summaries
        for s in summaries:
            assert s.metadata[PARENT_ID_FIELD] == page_key(
                NS, "objA", s.metadata["page_no"]
            )
            # parent_id is a DISTINCT field from the multi-vector doc_id pointer.
            assert s.metadata[PARENT_ID_FIELD] != s.metadata.get("doc_id")
            # Pages never leak into the algorithm's vector store.
            assert PAGE_KEY_FIELD not in s.metadata

    @pytest.mark.asyncio
    async def test_parent_id_rides_onto_docstore_originals(self) -> None:
        (
            pipeline,
            _,
            document_index,
            docstore_rm,
            _,
        ) = await _make_semi_structured_pipeline()
        await pipeline.aprocess(
            PagedInput(
                pages=[_page("objA", 1), _page("objA", 2)],
                chunks=_distinct_chunks("objA"),
            )
        )

        # The MultiVectorRetriever path returns the docstore ORIGINALS; they too
        # must carry parent_id so the outer ParentPageRetriever can deref them.
        keys = await docstore_rm.alist_keys()
        originals = await document_index.aget(keys)
        assert originals and all(o is not None for o in originals)
        for o in originals:
            assert o.metadata[PARENT_ID_FIELD] == page_key(
                NS, "objA", o.metadata["page_no"]
            )
            # doc_id is a summary-only pointer; the original carries only its own
            # parent_id — proving the two are orthogonal, not aliased.
            assert "doc_id" not in o.metadata

    @pytest.mark.asyncio
    async def test_deref_returns_parent_pages_end_to_end(self) -> None:
        pipeline, vectorstore, _, _, page_store = await _make_semi_structured_pipeline()
        await pipeline.aprocess(
            PagedInput(
                pages=[_page("objA", 1), _page("objA", 2)],
                chunks=_distinct_chunks("objA"),
            )
        )

        retriever = ParentPageRetriever(
            byte_store=page_store,
            base_retriever=vectorstore.as_retriever(search_kwargs={"k": 10}),
            id_key=PARENT_ID_FIELD,
        )
        pages = await retriever.ainvoke("revenue costs")

        # 3 summaries across 2 pages → the 2 distinct parent pages, deduped.
        assert {p.page_content for p in pages} == {
            "objA page 1 markdown",
            "objA page 2 markdown",
        }

    @pytest.mark.asyncio
    async def test_delete_source_evicts_pages_with_real_inner(self) -> None:
        """Eviction holds over the real algorithm too: deleting an object drops
        its cached pages (no storage bloat) while a sibling object survives."""
        pipeline, _, _, docstore_rm, page_store = await _make_semi_structured_pipeline()
        await pipeline.aprocess(
            PagedInput(
                pages=[_page("objA", 1), _page("objB", 1)],
                chunks=[_chunk("objA", 1, "a"), _chunk("objB", 1, "b")],
            )
        )

        await pipeline.adelete_source("objA")

        # objA's page evicted from the page store; objB's page untouched.
        assert sorted([k async for k in page_store.ayield_keys()]) == ["ns1/objB/p1"]
        # Inner (SemiStructured) cleared objA's docstore originals too.
        assert await docstore_rm.alist_keys(group_ids=["objA"]) == []
