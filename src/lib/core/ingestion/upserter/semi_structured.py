from typing import List, Literal

from typing_extensions import override

from langchain.indexes import aindex as lc_aindex
from langchain.indexes import index as lc_index
from langchain_core.indexing import RecordManager
from langchain_core.vectorstores import VectorStore

from src.lib.core.ingestion.types import SplitChunks
from src.lib.core.ingestion.upserter.base import Upserter

__all__ = [
    "SemiStructuredUpserter",
]


class SemiStructuredUpserter(Upserter[SplitChunks, List[str]]):
    """Upserter for semi-structured RAG pipelines.

    Handles pre-split text and table chunks, upserting them to the vectorstore.
    The separation is already done by the indexer, so no re-classification
    is needed here.

    When a ``record_manager`` is provided, deduplication and cleanup are
    handled via LangChain's ``index`` / ``aindex`` helpers.  In that case
    the upserter returns an empty list (``aindex`` does not expose inserted
    IDs via its ``IndexingResult``).

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
    def upsert(self, data: SplitChunks) -> List[str]:
        """Upsert split chunks to the vectorstore synchronously.

        Args:
            data: SplitChunks containing text and table chunks

        Returns:
            List of document IDs that were upserted, or an empty list when
            a record manager is used (deduplication via ``index``).
        """
        all_chunks = [*data.text_chunks, *data.table_chunks]
        if not all_chunks:
            return []

        if self._record_manager is not None:
            lc_index(
                all_chunks,
                self._record_manager,
                self._vectorstore,
                cleanup=self._cleanup,
                source_id_key=self._source_id_key,
                batch_size=self._batch_size,
            )
            return []

        ids: List[str] = []
        if data.text_chunks:
            ids.extend(self._vectorstore.add_documents(data.text_chunks))
        if data.table_chunks:
            ids.extend(self._vectorstore.add_documents(data.table_chunks))
        return ids

    @override
    async def aupsert(self, data: SplitChunks) -> List[str]:
        """Upsert split chunks to the vectorstore asynchronously.

        Args:
            data: SplitChunks containing text and table chunks

        Returns:
            List of document IDs that were upserted, or an empty list when
            a record manager is used (deduplication via ``aindex``).
        """
        all_chunks = [*data.text_chunks, *data.table_chunks]
        if not all_chunks:
            return []

        if self._record_manager is not None:
            await lc_aindex(
                all_chunks,
                self._record_manager,
                self._vectorstore,
                cleanup=self._cleanup,
                source_id_key=self._source_id_key,
                batch_size=self._batch_size,
            )
            return []

        ids: List[str] = []
        if data.text_chunks:
            text_ids = await self._vectorstore.aadd_documents(data.text_chunks)
            ids.extend(text_ids)
        if data.table_chunks:
            table_ids = await self._vectorstore.aadd_documents(data.table_chunks)
            ids.extend(table_ids)
        return ids
