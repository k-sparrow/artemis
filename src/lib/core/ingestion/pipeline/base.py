from abc import ABC, abstractmethod
from typing import Generic

from src.lib.core.ingestion.indexer import Indexer
from src.lib.core.ingestion.normalizer import DocumentNormalizer
from src.lib.core.ingestion.upserter import Upserter
from src.lib.core.ingestion.types import (
    InputT,
    OutputT,
    ProcessedT,
)


__all__ = [
    "BasePipeline",
    "Pipeline",
]


class BasePipeline(ABC, Generic[InputT, OutputT]):
    """Abstract base class defining the pipeline interface.

    Type Parameters:
        InputT: The type of input data fed into the pipeline
        OutputT: The type of output result from the pipeline
    """

    @abstractmethod
    def process(self, data: InputT) -> OutputT:
        """Process and upsert data synchronously.

        Args:
            data: Raw input data to process

        Returns:
            Result of the pipeline processing
        """

    @abstractmethod
    async def aprocess(self, data: InputT) -> OutputT:
        """Process and upsert data asynchronously.

        Args:
            data: Raw input data to process

        Returns:
            Result of the pipeline processing
        """


class Pipeline(BasePipeline[InputT, OutputT], Generic[InputT, ProcessedT, OutputT]):
    """Composable indexing pipeline.

    A Pipeline orchestrates the document ingestion flow by combining
    an Indexer (for processing/chunking) and an Upserter (for storage).

    Type Parameters:
        InputT: The type of input data (e.g., Sequence[Document])
        ProcessedT: The intermediate type between indexer and upserter
        OutputT: The type of output result (e.g., List[str] IDs)

    The pipeline is composable - it receives an Indexer and Upserter
    instance at initialization time.
    """

    def __init__(
        self,
        *,
        indexer: Indexer[InputT, ProcessedT],
        upserter: Upserter[ProcessedT, OutputT],
        normalizer: DocumentNormalizer | None = None,
    ):
        """Initialize the pipeline.

        Args:
            indexer: Indexer instance for data processing
            upserter: Upserter instance for data storage
            normalizer: Optional normalizer applied to documents before the
                indexer runs.  Use this to stamp metadata fields (e.g.
                namespace, source_type) onto every document without coupling
                that logic to the indexer or the caller.
        """
        self._indexer = indexer
        self._upserter = upserter
        self._normalizer = normalizer

    @property
    def indexer(self) -> Indexer[InputT, ProcessedT]:
        return self._indexer

    @property
    def upserter(self) -> Upserter[ProcessedT, OutputT]:
        return self._upserter

    @property
    def normalizer(self) -> DocumentNormalizer | None:
        return self._normalizer

    def process(self, data: InputT) -> OutputT:
        if self._normalizer is not None:
            data = self._normalizer.normalize(data)  # type: ignore[assignment]
        processed = self._indexer.process(data)
        return self._upserter.upsert(processed)

    async def aprocess(self, data: InputT) -> OutputT:
        if self._normalizer is not None:
            data = await self._normalizer.anormalize(data)  # type: ignore[assignment]
        processed = await self._indexer.aprocess(data)
        return await self._upserter.aupsert(processed)
