from abc import ABC, abstractmethod
from typing import Generic

from src.lib.core.ingestion.types import (
    ProcessedT,
    UpsertResult,
)


__all__ = [
    "Upserter",
]


class Upserter(ABC, Generic[ProcessedT]):
    """Base class for document upserters.

    An Upserter receives processed data from an Indexer and persists
    it to the appropriate storage backend(s).

    Type Parameters:
        ProcessedT: The type of processed input (e.g., List[Document], chunks)

    For Simple pipelines: stores chunks in vectorstore
    For SemiStructured pipelines: stores parent docs in docstore,
                                   child chunks in vectorstore
    """

    @abstractmethod
    def upsert(self, data: ProcessedT) -> UpsertResult:
        """Upsert data synchronously.

        Args:
            data: Processed data to upsert

        Returns:
            Result of the upsert operation
        """

    @abstractmethod
    async def aupsert(self, data: ProcessedT) -> UpsertResult:
        """Upsert data asynchronously.

        Args:
            data: Processed data to upsert

        Returns:
            Result of the upsert operation
        """
