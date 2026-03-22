from abc import ABC, abstractmethod
from typing import Generic

from src.lib.core.ingestion.indexer import Indexer
from src.lib.core.ingestion.upserter import Upserter
from src.lib.core.ingestion.types import (
    InputT,
    ProcessedT,
    UpsertResult,
)


__all__ = [
    "BasePipeline",
    "Pipeline",
]


class BasePipeline(ABC, Generic[InputT]):
    """Abstract base class defining the pipeline interface.

    Type Parameters:
        InputT: The type of input data fed into the pipeline
    """

    @abstractmethod
    def process(self, data: InputT) -> UpsertResult:
        """Process and upsert data synchronously.

        Args:
            data: Raw input data to process

        Returns:
            Result of the pipeline processing
        """

    @abstractmethod
    async def aprocess(self, data: InputT) -> UpsertResult:
        """Process and upsert data asynchronously.

        Args:
            data: Raw input data to process

        Returns:
            Result of the pipeline processing
        """

    @abstractmethod
    def delete_source(self, source: str) -> None:
        """Delete all chunks for *source* from the vectorstore and record manager."""

    @abstractmethod
    async def adelete_source(self, source: str) -> None:
        """Async version of :meth:`delete_source`."""


class Pipeline(BasePipeline[InputT], Generic[InputT, ProcessedT]):
    """Composable indexing pipeline.

    A Pipeline orchestrates the document ingestion flow by combining
    an Indexer (for processing/chunking) and an Upserter (for storage).

    Type Parameters:
        InputT: The type of input data (e.g., Sequence[Document])
        ProcessedT: The intermediate type between indexer and upserter

    The pipeline is composable - it receives an Indexer and Upserter
    instance at initialization time.
    """

    def __init__(
        self,
        *,
        indexer: Indexer[InputT, ProcessedT],
        upserter: Upserter[ProcessedT],
    ):
        """Initialize the pipeline.

        Args:
            indexer: Indexer instance for data processing
            upserter: Upserter instance for data storage
        """
        self._indexer = indexer
        self._upserter = upserter

    @property
    def indexer(self) -> Indexer[InputT, ProcessedT]:
        return self._indexer

    @property
    def upserter(self) -> Upserter[ProcessedT]:
        return self._upserter

    def process(self, data: InputT) -> UpsertResult:
        processed = self._indexer.process(data)
        return self._upserter.upsert(processed)

    async def aprocess(self, data: InputT) -> UpsertResult:
        processed = await self._indexer.aprocess(data)
        return await self._upserter.aupsert(processed)

    def delete_source(self, source: str) -> None:
        self._upserter.delete_source(source)

    async def adelete_source(self, source: str) -> None:
        await self._upserter.adelete_source(source)
