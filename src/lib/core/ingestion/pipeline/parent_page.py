# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Parent-page indexing as a composable pipeline decorator.

``ParentPagePipeline`` is a :class:`BasePipeline` that **wraps an inner
``BasePipeline``** and adds parent-page caching *around* it, leaving the inner
algorithm (Simple, SemiStructured, …) completely untouched — the same way a
reranker composes over search. BEFORE delegating it (a) writes the page parents
to a side doc store and (b) stamps ``parent_id`` on each chunk; then it hands the
chunks to ``inner.aprocess``. Deletion prefix-deletes the pages and delegates.

The pages are written through ``lc_aindex`` (so unchanged pages are *skipped* —
free diffing, no child re-embed cascade) but keyed **deterministically** by
``page_key`` via ``upsert_kwargs={"key": PAGE_KEY_FIELD}`` (the injected
``document_index`` must be a ``MetadataKeyedDocumentIndex``). Because the record
manager then tracks content-hash uids while the store is keyed deterministically,
``lc_index``'s uid cleanup MISSES the pages — so page deletion is **prefix-based**
on the byte store (see ``ingestion/RECORD_MANAGER.md`` §9).

Depends only on abstractions: ``BasePipeline`` / ``RecordManager`` /
``DocumentIndex`` / ``ByteStore`` — never on a concrete algorithm.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import List

from langchain.indexes import aindex as lc_aindex
from langchain_core.documents import Document
from langchain_core.indexing import RecordManager
from langchain_core.indexing.base import DocumentIndex
from langchain_core.stores import ByteStore

from src.lib.core.ingestion.pipeline.base import BasePipeline
from src.lib.core.ingestion.types import UpsertResult

__all__ = [
    "ParentPagePipeline",
    "PagedInput",
    "ParentKind",
    "page_key",
    "PAGE_KEY_FIELD",
    "PARENT_ID_FIELD",
    "PARENT_KIND_FIELD",
]

# Metadata field names — the contract between the index decorator (which stamps)
# and the retrieve decorator (which reads ``PARENT_ID_FIELD`` as its ``id_key``).
PAGE_KEY_FIELD = "page_key"  # a page's own deterministic store key
PARENT_ID_FIELD = "parent_id"  # a chunk's pointer at its parent page
PARENT_KIND_FIELD = "parent_kind"
# Fields the parent-page layer reads off each document's metadata.
_OBJ_ID_FIELD = "obj_id"
_PAGE_NO_FIELD = "page_no"


class ParentKind(StrEnum):
    """What a ``parent_id`` dereferences to (observability/routing; not load-bearing)."""

    PAGE = "page"


def page_key(namespace_id: str, obj_id: str, page_no: int) -> str:
    """Deterministic doc-store key for a parent page — THE single source of truth.

    Used for BOTH ``page.metadata[PAGE_KEY_FIELD]`` (the store key) AND
    ``chunk.metadata[PARENT_ID_FIELD]`` (the pointer), so they are byte-identical
    (drift ⇒ a silent retrieval miss). The ``{ns}/{obj}/`` prefix structure is what
    makes per-object and per-namespace deletion clean prefix operations.
    """
    return f"{namespace_id}/{obj_id}/p{page_no}"


@dataclass
class PagedInput:
    """Input to :class:`ParentPagePipeline` — the page parents alongside the
    chunks the inner pipeline embeds. An empty instance is the namespace wipe
    (mirrors ``BasePipeline.aprocess([])``)."""

    pages: List[Document] = field(default_factory=list)
    chunks: List[Document] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.pages and not self.chunks


class ParentPagePipeline(BasePipeline[PagedInput]):
    """Wrap an inner ``BasePipeline`` with parent-page caching + linking."""

    def __init__(
        self,
        inner: BasePipeline,
        *,
        record_manager: RecordManager,
        document_index: DocumentIndex,
        byte_store: ByteStore,
        namespace_id: str,
    ) -> None:
        self._inner = inner
        self._record_manager = record_manager
        self._document_index = document_index
        self._byte_store = byte_store
        self._namespace_id = namespace_id

    # ------------------------------------------------------------------ async

    async def aprocess(self, data: PagedInput) -> UpsertResult:
        # Namespace wipe: empty input ⇒ clear pages + delegate the inner wipe.
        if data.is_empty():
            await self._awipe_namespace()
            return await self._inner.aprocess([])

        pages, chunks = data.pages, data.chunks

        # 1. Stamp each page's deterministic store key, then write via lc_aindex
        #    (free diffing — unchanged pages skipped) under that deterministic key.
        for page in pages:
            page.metadata[PAGE_KEY_FIELD] = self._page_key(page)
        await lc_aindex(
            pages,
            self._record_manager,
            self._document_index,
            cleanup="scoped_full",
            source_id_key=_OBJ_ID_FIELD,
            upsert_kwargs={"key": PAGE_KEY_FIELD},
        )

        # 2. Reconcile the store per object — lc_index's uid-based adelete misses
        #    deterministic keys (§9), so drop any page key under {ns}/{obj}/ that is
        #    no longer present (handles a shrinking page count).
        await self._areconcile(pages)

        # 3. Stamp parent_id on chunks (same page_key fn → byte-identical string).
        self._stamp_parents(pages, chunks)

        # 4. Delegate embedding to the inner pipeline (algorithm untouched).
        #
        # chunks == [] here is NOT the namespace-wipe case (data.is_empty() is
        # already False, since pages is non-empty) — it means these specific
        # object(s) legitimately have zero chunks this run. self._inner.aprocess([])
        # would call the upserter's aupsert([]), which calls LangChain's
        # aindex(docs_source=[], cleanup="scoped_full", ...): with nothing to
        # iterate, its internal source-id set stays empty, becoming
        # group_ids=[] in SQLRecordManager.list_keys/alist_keys — an empty
        # list reads as falsy there, so the group filter is skipped entirely
        # and cleanup reaps every key in the WHOLE namespace, not just these
        # objects'. adelete_source is already correctly, individually scoped
        # (group_ids=[source], never empty) for both Simple and SemiStructured
        # upserters, so use that instead — this exact path caused a production
        # namespace-wide vector wipe (a single object's chunking degrading to
        # zero chunks was enough to reap every other object's vectors too).
        if not chunks:
            obj_ids = {page.metadata[_OBJ_ID_FIELD] for page in pages}
            for obj_id in obj_ids:
                await self._inner.adelete_source(obj_id)
            return UpsertResult(
                num_added=0, num_updated=0, num_skipped=0, num_deleted=0, ids=[]
            )

        return await self._inner.aprocess(chunks)

    async def adelete_source(self, source: str) -> None:
        # Pages: prefix-delete the store, then clear the page RM rows. lc_index's
        # uid adelete would miss the deterministic keys, so we do it directly (§9).
        await self._aprefix_delete(f"{self._namespace_id}/{source}/")
        rm_keys = await self._record_manager.alist_keys(group_ids=[source])
        if rm_keys:
            await self._record_manager.adelete_keys(rm_keys)
        # Children: delegate to the inner (algorithm-owned) deletion.
        await self._inner.adelete_source(source)

    # ------------------------------------------------------------------- sync

    def process(self, data: PagedInput) -> UpsertResult:
        raise NotImplementedError("ParentPagePipeline is async-only; use aprocess")

    def delete_source(self, source: str) -> None:
        raise NotImplementedError(
            "ParentPagePipeline is async-only; use adelete_source"
        )

    # --------------------------------------------------------------- internals

    def _page_key(self, doc: Document) -> str:
        return page_key(
            self._namespace_id,
            doc.metadata[_OBJ_ID_FIELD],
            doc.metadata[_PAGE_NO_FIELD],
        )

    def _stamp_parents(self, pages: List[Document], chunks: List[Document]) -> None:
        """Point each chunk at its parent page. A chunk with no/absent provenance
        page falls back to its object's first page."""
        valid: dict[str, set[int]] = {}
        for page in pages:
            valid.setdefault(page.metadata[_OBJ_ID_FIELD], set()).add(
                page.metadata[_PAGE_NO_FIELD]
            )
        first = {obj: min(pns) for obj, pns in valid.items() if pns}
        for chunk in chunks:
            obj = chunk.metadata.get(_OBJ_ID_FIELD)
            pages_for_obj = valid.get(obj)
            if not pages_for_obj:
                continue  # no parent page for this object → leave the chunk unlinked
            page_no = chunk.metadata.get(_PAGE_NO_FIELD)
            if page_no not in pages_for_obj:
                page_no = first[obj]
            chunk.metadata[PARENT_ID_FIELD] = page_key(self._namespace_id, obj, page_no)
            chunk.metadata.setdefault(PARENT_KIND_FIELD, ParentKind.PAGE.value)

    async def _areconcile(self, pages: List[Document]) -> None:
        expected: dict[str, set[str]] = {}
        for page in pages:
            expected.setdefault(page.metadata[_OBJ_ID_FIELD], set()).add(
                page.metadata[PAGE_KEY_FIELD]
            )
        for obj, keys in expected.items():
            prefix = f"{self._namespace_id}/{obj}/"
            existing = [k async for k in self._byte_store.ayield_keys(prefix=prefix)]
            stale = [k for k in existing if k not in keys]
            if stale:
                await self._byte_store.amdelete(stale)

    async def _awipe_namespace(self) -> None:
        # Empty lc_aindex ⇒ full cleanup of the page RM rows (namespace-scoped);
        # then prefix-delete the deterministic-keyed objects the RM can't reach.
        await lc_aindex(
            [],
            self._record_manager,
            self._document_index,
            cleanup="scoped_full",
            source_id_key=_OBJ_ID_FIELD,
        )
        await self._aprefix_delete(f"{self._namespace_id}/")

    async def _aprefix_delete(self, prefix: str) -> None:
        keys = [k async for k in self._byte_store.ayield_keys(prefix=prefix)]
        if keys:
            await self._byte_store.amdelete(keys)
