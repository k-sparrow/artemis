from abc import ABC, abstractmethod
from typing import Generic

from src.lib.core.ingestion.types import (
    OutputT,
    ProcessedT,
)


__all__ = [
    "Upserter",
]


class Upserter(ABC, Generic[ProcessedT, OutputT]):
    """Base class for document upserters.

    An Upserter receives processed data from an Indexer and persists
    it to the appropriate storage backend(s).

    Type Parameters:
        ProcessedT: The type of processed input (e.g., List[Document], chunks)
        OutputT: The type of output result (e.g., List[str] IDs, status objects)

    For Simple pipelines: stores chunks in vectorstore
    For SemiStructured pipelines: stores parent docs in docstore,
                                   child chunks in vectorstore
    """

    @abstractmethod
    def upsert(self, data: ProcessedT) -> OutputT:
        """Upsert data synchronously.

        Args:
            data: Processed data to upsert

        Returns:
            Result of the upsert operation (e.g., document IDs)
        """

    @abstractmethod
    async def aupsert(self, data: ProcessedT) -> OutputT:
        """Upsert data asynchronously.

        Args:
            data: Processed data to upsert

        Returns:
            Result of the upsert operation (e.g., document IDs)
        """
