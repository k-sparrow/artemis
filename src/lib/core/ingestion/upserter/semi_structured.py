# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

import uuid
from typing import List, Literal, Optional

from typing_extensions import override

from langchain.indexes import aindex as lc_aindex
from langchain.indexes import index as lc_index
from langchain_core.documents import Document
from langchain_core.indexing import RecordManager
from langchain_core.indexing.base import DocumentIndex
from langchain_core.runnables import Runnable
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.types import SplitChunks, UpsertResult
from src.lib.core.ingestion.upserter._proxy import (
    _IDCapturingDocumentIndex,
    _IDCapturingVectorStore,
)
from src.lib.core.ingestion.upserter.base import Upserter

__all__ = [
    "SemiStructuredUpserter",
]


class SemiStructuredUpserter(Upserter[SplitChunks]):
    """Upserter for semi-structured RAG pipelines.

    Operates in one of two modes depending on whether a ``document_index``
    is provided:

    **Direct mode** (``document_index`` is ``None``):
        Text and table chunks are embedded straight into the vectorstore.
        Optional deduplication via ``record_manager``.

    **Multi-vector mode** (``document_index`` is provided):
        Implements the semi-structured RAG pattern from the LangChain cookbook.
        The upserter handles the full 4-step flow:

        1. Write originals to ``document_index`` via ``lc_index``/``lc_aindex``
           (with optional deduplication via ``docstore_record_manager``).  A
           proxy captures the lc_hash storage keys of documents that were
           *actually written* (not skipped as duplicates).
        2. Fetch those originals back from the docstore by lc_hash.
        3. Summarize the fetched originals per content type using
           ``table_summarizer`` / ``text_summarizer``.
        4. Build summary :class:`Document` objects with
           ``metadata[id_key] = lc_hash`` and embed them in the vectorstore.

        Because summaries are only generated for originals that changed (step 1
        skips unchanged docs), a non-deterministic LLM produces new embeddings
        only when the source content changes.  The old summary embedding in the
        vectorstore already carries the correct ``lc_hash`` pointer and remains
        valid.

        When ``docstore_record_manager`` is absent, originals are written
        directly via :meth:`DocumentIndex.upsert` and all chunks are
        summarized.
    """

    def __init__(
        self,
        vectorstore: VectorStore,
        document_index: DocumentIndex | None = None,
        docstore_record_manager: RecordManager | None = None,
        record_manager: RecordManager | None = None,
        id_key: str = "doc_id",
        cleanup: Literal["full", "incremental", "scoped_full", "none"] = "scoped_full",
        source_id_key: str = "source",
        batch_size: int = 32,
        table_summarizer: Optional[Runnable] = None,
        text_summarizer: Optional[Runnable] = None,
    ):
        self._vectorstore = vectorstore
        self._document_index = document_index
        self._docstore_record_manager = docstore_record_manager
        self._record_manager = record_manager
        self._id_key = id_key
        # lc_index / lc_aindex expect None (no cleanup), not the string "none"
        self._cleanup: Literal["full", "incremental", "scoped_full"] | None = (
            None if cleanup == "none" else cleanup
        )
        self._source_id_key = source_id_key
        self._batch_size = batch_size
        self._table_summarizer = table_summarizer
        self._text_summarizer = text_summarizer

    @property
    def vectorstore(self) -> VectorStore:
        return self._vectorstore

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _embed_summaries_sync(self, summaries: List[Document]) -> UpsertResult:
        """Write summaries to the vectorstore (sync), with optional dedup."""
        if self._record_manager is not None:
            proxy = _IDCapturingVectorStore(self._vectorstore)
            result = lc_index(
                summaries,
                self._record_manager,
                proxy,
                cleanup=self._cleanup,
                source_id_key=self._source_id_key,
                batch_size=self._batch_size,
            )
            return UpsertResult(
                num_added=result["num_added"],
                num_updated=result["num_updated"],
                num_skipped=result["num_skipped"],
                num_deleted=result["num_deleted"],
                ids=proxy.drain(),
            )

        ids = self._vectorstore.add_documents(summaries) if summaries else []
        return UpsertResult(num_added=len(ids), ids=ids)

    async def _embed_summaries_async(self, summaries: List[Document]) -> UpsertResult:
        """Write summaries to the vectorstore (async), with optional dedup."""
        if self._record_manager is not None:
            proxy = _IDCapturingVectorStore(self._vectorstore)
            result = await lc_aindex(
                summaries,
                self._record_manager,
                proxy,
                cleanup=self._cleanup,
                source_id_key=self._source_id_key,
                batch_size=self._batch_size,
            )
            return UpsertResult(
                num_added=result["num_added"],
                num_updated=result["num_updated"],
                num_skipped=result["num_skipped"],
                num_deleted=result["num_deleted"],
                ids=proxy.drain(),
            )

        ids = await self._vectorstore.aadd_documents(summaries) if summaries else []
        return UpsertResult(num_added=len(ids), ids=ids)

    def _build_summaries_sync(
        self, id_doc_pairs: List[tuple[str, Document]]
    ) -> List[Document]:
        """Summarize (id, original) pairs and return summary Documents.

        Batches text and table chunks separately so each summarizer receives
        only the content type it was designed for.  The ``id_key`` metadata
        field on each summary is set to the storage key (lc_hash or UUID) so
        ``MultiVectorRetriever`` can resolve the original at query time.
        """
        text_pairs: List[tuple[str, Document]] = []
        table_pairs: List[tuple[str, Document]] = []
        for doc_id, doc in id_doc_pairs:
            if doc.metadata.get("type") == "table":
                table_pairs.append((doc_id, doc))
            else:
                text_pairs.append((doc_id, doc))

        summaries: List[Document] = []

        if text_pairs:
            texts = [d.page_content for _, d in text_pairs]
            summary_texts: List[str] = (
                self._text_summarizer.batch(texts)
                if self._text_summarizer is not None
                else texts
            )
            for (doc_id, doc), summary_text in zip(text_pairs, summary_texts):
                summaries.append(
                    Document(
                        page_content=summary_text,
                        metadata={**doc.metadata, self._id_key: doc_id},
                    )
                )

        if table_pairs:
            texts = [d.page_content for _, d in table_pairs]
            summary_texts = (
                self._table_summarizer.batch(texts)
                if self._table_summarizer is not None
                else texts
            )
            for (doc_id, doc), summary_text in zip(table_pairs, summary_texts):
                summaries.append(
                    Document(
                        page_content=summary_text,
                        metadata={**doc.metadata, self._id_key: doc_id},
                    )
                )

        return summaries

    async def _build_summaries_async(
        self, id_doc_pairs: List[tuple[str, Document]]
    ) -> List[Document]:
        """Async version of :meth:`_build_summaries_sync`.

        Uses ``Runnable.abatch`` so LLM calls are issued concurrently.
        """
        text_pairs: List[tuple[str, Document]] = []
        table_pairs: List[tuple[str, Document]] = []
        for doc_id, doc in id_doc_pairs:
            if doc.metadata.get("type") == "table":
                table_pairs.append((doc_id, doc))
            else:
                text_pairs.append((doc_id, doc))

        summaries: List[Document] = []

        if text_pairs:
            texts = [d.page_content for _, d in text_pairs]
            summary_texts: List[str] = (
                await self._text_summarizer.abatch(texts)
                if self._text_summarizer is not None
                else texts
            )
            for (doc_id, doc), summary_text in zip(text_pairs, summary_texts):
                summaries.append(
                    Document(
                        page_content=summary_text,
                        metadata={**doc.metadata, self._id_key: doc_id},
                    )
                )

        if table_pairs:
            texts = [d.page_content for _, d in table_pairs]
            summary_texts = (
                await self._table_summarizer.abatch(texts)
                if self._table_summarizer is not None
                else texts
            )
            for (doc_id, doc), summary_text in zip(table_pairs, summary_texts):
                summaries.append(
                    Document(
                        page_content=summary_text,
                        metadata={**doc.metadata, self._id_key: doc_id},
                    )
                )

        return summaries

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @override
    def delete_source(self, source: str) -> None:
        # Multi-vector: clean docstore originals via its RM first
        if self._document_index is not None and self._docstore_record_manager is not None:
            doc_keys = self._docstore_record_manager.list_keys(group_ids=[source])
            if doc_keys:
                self._document_index.delete(doc_keys)
                self._docstore_record_manager.delete_keys(doc_keys)
        # Clean vectorstore + its RM (both direct and multi-vector modes)
        if self._record_manager is not None:
            vs_keys = self._record_manager.list_keys(group_ids=[source])
            if vs_keys:
                self._vectorstore.delete(vs_keys)
                self._record_manager.delete_keys(vs_keys)

    @override
    async def adelete_source(self, source: str) -> None:
        # Multi-vector: clean docstore originals via its RM first
        if self._document_index is not None and self._docstore_record_manager is not None:
            doc_keys = await self._docstore_record_manager.alist_keys(group_ids=[source])
            if doc_keys:
                await self._document_index.adelete(doc_keys)
                await self._docstore_record_manager.adelete_keys(doc_keys)
        # Clean vectorstore + its RM (both direct and multi-vector modes)
        if self._record_manager is not None:
            vs_keys = await self._record_manager.alist_keys(group_ids=[source])
            if vs_keys:
                await self._vectorstore.adelete(vs_keys)
                await self._record_manager.adelete_keys(vs_keys)

    @override
    def upsert(self, data: SplitChunks) -> UpsertResult:
        if self._document_index is not None:
            return self._upsert_multi_vector(data)
        return self._upsert_direct(data)

    @override
    async def aupsert(self, data: SplitChunks) -> UpsertResult:
        if self._document_index is not None:
            return await self._aupsert_multi_vector(data)
        return await self._aupsert_direct(data)

    # ------------------------------------------------------------------
    # Direct mode (no docstore)
    # ------------------------------------------------------------------

    def _upsert_direct(self, data: SplitChunks) -> UpsertResult:
        all_chunks = [*data.text_chunks, *data.table_chunks]
        return self._embed_summaries_sync(all_chunks)

    async def _aupsert_direct(self, data: SplitChunks) -> UpsertResult:
        all_chunks = [*data.text_chunks, *data.table_chunks]
        return await self._embed_summaries_async(all_chunks)

    # ------------------------------------------------------------------
    # Multi-vector mode (docstore + summarization)
    # ------------------------------------------------------------------

    def _upsert_multi_vector(self, data: SplitChunks) -> UpsertResult:
        all_chunks = list(data.text_chunks) + list(data.table_chunks)

        if self._docstore_record_manager is not None:
            # Step 1: write originals to docstore via lc_index; proxy captures
            # the lc_hash keys of documents that were *actually* written.
            proxy = _IDCapturingDocumentIndex(self._document_index)
            lc_index(
                all_chunks,
                self._docstore_record_manager,
                proxy,
                cleanup=self._cleanup,
                source_id_key=self._source_id_key,
                batch_size=self._batch_size,
            )
            # Step 2: drain — only IDs of newly/re-written docs
            written_ids = proxy.drain()

            # Step 3: fetch written originals back from the docstore
            if written_ids:
                fetched = self._document_index.get(written_ids)
                id_doc_pairs = [
                    (lc_id, doc)
                    for lc_id, doc in zip(written_ids, fetched)
                    if doc is not None
                ]
            else:
                id_doc_pairs = []
        else:
            # Direct docstore write: generate UUIDs ourselves and use them as
            # storage keys.  No deduplication — each call may overwrite.
            all_ids = [str(uuid.uuid4()) for _ in all_chunks]
            self._document_index.upsert(all_chunks, ids=all_ids)  # type: ignore[call-arg]
            id_doc_pairs = list(zip(all_ids, all_chunks))

        # Step 4: summarize fetched originals and embed in vectorstore
        summaries = self._build_summaries_sync(id_doc_pairs)
        return self._embed_summaries_sync(summaries)

    async def _aupsert_multi_vector(self, data: SplitChunks) -> UpsertResult:
        all_chunks = list(data.text_chunks) + list(data.table_chunks)

        if self._docstore_record_manager is not None:
            # Step 1: write originals to docstore via lc_aindex; proxy captures
            # the lc_hash keys of documents that were *actually* written.
            proxy = _IDCapturingDocumentIndex(self._document_index)
            await lc_aindex(
                all_chunks,
                self._docstore_record_manager,
                proxy,
                cleanup=self._cleanup,
                source_id_key=self._source_id_key,
                batch_size=self._batch_size,
            )
            # Step 2: drain — only IDs of newly/re-written docs
            written_ids = proxy.drain()

            # Step 3: fetch written originals back from the docstore
            if written_ids:
                fetched = await self._document_index.aget(written_ids)
                id_doc_pairs = [
                    (lc_id, doc)
                    for lc_id, doc in zip(written_ids, fetched)
                    if doc is not None
                ]
            else:
                id_doc_pairs = []
        else:
            # Direct docstore write: generate UUIDs ourselves and use them as
            # storage keys.  No deduplication — each call may overwrite.
            all_ids = [str(uuid.uuid4()) for _ in all_chunks]
            await self._document_index.aupsert(all_chunks, ids=all_ids)
            id_doc_pairs = list(zip(all_ids, all_chunks))

        # Step 4: summarize fetched originals and embed in vectorstore
        summaries = await self._build_summaries_async(id_doc_pairs)
        return await self._embed_summaries_async(summaries)
