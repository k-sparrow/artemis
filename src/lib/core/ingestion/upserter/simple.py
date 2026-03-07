# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from typing import List, Literal

from typing_extensions import override

from langchain.indexes import aindex as lc_aindex
from langchain.indexes import index as lc_index
from langchain_core.documents import Document
from langchain_core.indexing import RecordManager
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.types import UpsertResult
from src.lib.core.ingestion.upserter._proxy import _IDCapturingVectorStore
from src.lib.core.ingestion.upserter.base import Upserter


__all__ = [
    "SimpleUpserter",
]


class SimpleUpserter(Upserter[List[Document], UpsertResult]):
    """Simple upserter for standard RAG pipelines.

    Uses LangChain's ``index`` / ``aindex`` for deduplication and cleanup.
    IDs written to the vectorstore are captured via
    :class:`_IDCapturingVectorStore` and returned in :class:`UpsertResult`.
    """

    def __init__(
        self,
        vectorstore: VectorStore,
        record_manager: RecordManager,
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
    def upsert(self, data: List[Document]) -> UpsertResult:
        proxy = _IDCapturingVectorStore(self._vectorstore)
        result = lc_index(
            docs_source=data,
            record_manager=self._record_manager,
            vector_store=proxy,
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

    @override
    async def aupsert(self, data: List[Document]) -> UpsertResult:
        proxy = _IDCapturingVectorStore(self._vectorstore)
        result = await lc_aindex(
            docs_source=data,
            record_manager=self._record_manager,
            vector_store=proxy,
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
