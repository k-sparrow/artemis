from typing import Any, Sequence

from langchain_core.documents import Document

from src.lib.core.ingestion.normalizer.base import DocumentNormalizer


__all__ = [
    "MetadataFieldNormalizer",
]


class MetadataFieldNormalizer(DocumentNormalizer):
    """Normalizer that stamps fixed metadata fields onto every document.

    Fields are shallow-merged into each document's ``metadata`` dict.
    Existing keys are overwritten; keys absent from ``fields`` are left
    untouched.  The original documents are never mutated — each call
    returns a new list of new ``Document`` objects.

    Example::

        import uuid
        normalizer = MetadataFieldNormalizer(
            fields={"namespace": uuid.uuid4().hex, "source_type": "web"}
        )
        pipeline = Pipeline(indexer=..., upserter=..., normalizer=normalizer)
    """

    def __init__(self, fields: dict[str, Any]) -> None:
        """
        Args:
            fields: Mapping of metadata key → value to apply to every document.
        """
        self._fields = fields

    def normalize(self, docs: Sequence[Document]) -> list[Document]:
        return [
            Document(
                page_content=doc.page_content,
                metadata={**doc.metadata, **self._fields},
            )
            for doc in docs
        ]

    async def anormalize(self, docs: Sequence[Document]) -> list[Document]:
        # Pure in-memory transformation — no async I/O required.
        return self.normalize(docs)
