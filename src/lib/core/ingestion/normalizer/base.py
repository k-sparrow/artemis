from abc import ABC, abstractmethod
from typing import Sequence

from langchain_core.documents import Document


__all__ = [
    "DocumentNormalizer",
]


class DocumentNormalizer(ABC):
    """Base class for document normalizers.

    A normalizer runs *before* the indexer in a pipeline and applies
    field-level transformations to each document — typically enriching
    or overriding metadata fields (e.g. stamping a namespace UUID).

    Implementations must be stateless with respect to the document list
    so that the same normalizer instance can be reused across multiple
    pipeline invocations.
    """

    @abstractmethod
    def normalize(self, docs: Sequence[Document]) -> list[Document]:
        """Normalize documents synchronously.

        Args:
            docs: Input documents to normalize.

        Returns:
            New list of documents with normalization applied.
            The original documents are not mutated.
        """

    @abstractmethod
    async def anormalize(self, docs: Sequence[Document]) -> list[Document]:
        """Normalize documents asynchronously.

        Args:
            docs: Input documents to normalize.

        Returns:
            New list of documents with normalization applied.
            The original documents are not mutated.
        """
