# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from typing import List, Literal

from typing_extensions import override

from langchain.indexes import aindex as lc_aindex
from langchain.indexes import index as lc_index
from langchain_core.indexing import RecordManager
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.types import SplitChunks, UpsertResult
from src.lib.core.ingestion.upserter._proxy import _IDCapturingVectorStore
from src.lib.core.ingestion.upserter.base import Upserter

__all__ = [
    "SemiStructuredUpserter",
]


class SemiStructuredUpserter(Upserter[SplitChunks, UpsertResult]):
    """Upserter for semi-structured RAG pipelines.

    Handles pre-split text and table chunks, upserting them to the vectorstore.
    When a ``record_manager`` is provided, deduplication and cleanup are
    handled via LangChain's ``index`` / ``aindex`` helpers.  IDs written to
    the vectorstore are captured via :class:`_IDCapturingVectorStore` and
    returned in :class:`UpsertResult`.

    Without a ``record_manager``, chunks are added directly via
    ``add_documents`` / ``aadd_documents`` and their IDs are returned.
    """

    def __init__(
        self,
        vectorstore: VectorStore,
        record_manager: RecordManager | None = None,
        cleanup: Literal["full", "incremental", "none"] = "incremental",
        source_id_key: str = "source",
        batch_size: int = 32,
    ):
        self._vectorstore = vectorstore
        self._record_manager = record_manager
        # lc_index / lc_aindex expect None (no cleanup), not the string "none"
        self._cleanup: Literal["full", "incremental"] | None = (
            None if cleanup == "none" else cleanup
        )
        self._source_id_key = source_id_key
        self._batch_size = batch_size

    @property
    def vectorstore(self) -> VectorStore:
        return self._vectorstore

    @override
    def upsert(self, data: SplitChunks) -> UpsertResult:
        all_chunks = [*data.text_chunks, *data.table_chunks]

        if self._record_manager is not None:
            proxy = _IDCapturingVectorStore(self._vectorstore)
            result = lc_index(
                all_chunks,
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

        ids: List[str] = []
        if data.text_chunks:
            ids.extend(self._vectorstore.add_documents(data.text_chunks))
        if data.table_chunks:
            ids.extend(self._vectorstore.add_documents(data.table_chunks))
        return UpsertResult(num_added=len(ids), ids=ids)

    @override
    async def aupsert(self, data: SplitChunks) -> UpsertResult:
        all_chunks = [*data.text_chunks, *data.table_chunks]

        if self._record_manager is not None:
            proxy = _IDCapturingVectorStore(self._vectorstore)
            result = await lc_aindex(
                all_chunks,
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

        ids: List[str] = []
        if data.text_chunks:
            ids.extend(await self._vectorstore.aadd_documents(data.text_chunks))
        if data.table_chunks:
            ids.extend(await self._vectorstore.aadd_documents(data.table_chunks))
        return UpsertResult(num_added=len(ids), ids=ids)
