from abc import ABC, abstractmethod
from typing import Generic

from src.backend.indexing.lib.processing.types import (
    InputT,
    ProcessedT,
)


__all__ = [
    "Indexer",
]


class Indexer(ABC, Generic[InputT, ProcessedT]):
    """Base class for document indexers.

    An Indexer receives input data and processes it according to a specific
    indexing algorithm (e.g., chunking strategy).

    Type Parameters:
        InputT: The type of input data (e.g., Sequence[Document], file paths)
        ProcessedT: The type of processed output (e.g., List[Document], chunks)

    The output is processed data ready for upserting.
    """

    @abstractmethod
    def process(self, data: InputT) -> ProcessedT:
        """Process input data synchronously.

        Args:
            data: Raw input data to process

        Returns:
            Processed data (e.g., chunked documents)
        """

    @abstractmethod
    async def aprocess(self, data: InputT) -> ProcessedT:
        """Process input data asynchronously.

        Args:
            data: Raw input data to process

        Returns:
            Processed data (e.g., chunked documents)
        """
