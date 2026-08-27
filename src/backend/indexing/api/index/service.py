from typing import List, Optional
from uuid import UUID

from langchain_core.documents import Document
from pydantic import BaseModel

from src.lib.core.ingestion import BasePipeline
from src.lib.core.ingestion.contract import BlobRef
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.pipeline.parent_page import PagedInput
from src.lib.core.ingestion.types import (
    Page,
    ParseArtifact,
    ParsedChunk,
    UpsertResult,
)


class IngestRequest(BaseModel):
    """Inline-or-reference ingest input: exactly one of ``artifact_ref`` / ``chunks``."""

    artifact_ref: Optional[BlobRef] = None
    chunks: Optional[List[ParsedChunk]] = None


def decode_artifact(data: bytes) -> ParseArtifact:
    """Deserialise a :class:`ParseArtifact` (pages + chunks) from JSON bytes."""
    return ParseArtifact.model_validate_json(data)


async def a_delete(
    namespace: UUID,
    pipeline: BasePipeline,
    obj_id: Optional[str] = None,
) -> None:
    """Delete documents for *namespace* through the (parent-page-wrapped) pipeline.

    If *obj_id* is provided, only that object is removed — chunks (from the
    vectorstore + record manager) and its cached parent pages — via
    :meth:`pipeline.adelete_source`.

    If *obj_id* is ``None`` (namespace-level deletion), the pipeline is called
    with an empty :class:`PagedInput`.  The wrapping ``ParentPagePipeline`` reads
    that as a namespace wipe (clear the page store + page record manager) and
    delegates an empty document list to the inner pipeline, whose upserters use
    ``cleanup="scoped_full"`` with an empty ``scoped_full_cleanup_source_ids`` —
    so LangChain's ``aindex`` falls back to unscoped full cleanup, deleting every
    key tracked in the namespace's record manager and its vectorstore entries.
    """
    if obj_id is not None:
        await pipeline.adelete_source(obj_id)
    else:
        await pipeline.aprocess(PagedInput())


async def a_index_and_ingest(
    chunks: List[ParsedChunk],
    pages: List[Page],
    pipeline: BasePipeline,
    namespace: UUID,
    group_id: Optional[str] = None,
) -> UpsertResult:
    """Index a parse artifact (chunks + parent pages) into the vectorstore.

    1. Convert each ParsedChunk to a Document, carrying ``obj_id`` and ``page_no``
       (the latter links the chunk to its parent page).
    2. Convert each Page to a parent Document, carrying its own ``obj_id`` (the
       page's source object) and ``page_no``.
    3. Stamp ``namespace_id`` (and ``group_id`` when present) onto both via the
       :class:`MetadataFieldNormalizer`.
    4. Hand both to the pipeline as a :class:`PagedInput`.  The wrapping
       ``ParentPagePipeline`` caches the pages, stamps ``parent_id`` on each
       chunk, then delegates the chunks to the inner algorithm (embed + upsert).
    """
    chunk_docs = [
        Document(
            page_content=chunk.page_content,
            metadata={
                "source": chunk.source,
                "type": chunk.type,
                "obj_id": str(chunk.obj_id),
                "page_no": chunk.page_no,
            },
        )
        for chunk in chunks
    ]

    # Pages are self-describing — each carries its own obj_id and source (set
    # at parse time).
    page_docs = [
        Document(
            page_content=page.markdown,
            metadata={
                "source": page.source,
                "obj_id": str(page.obj_id),
                "page_no": page.page_no,
            },
        )
        for page in pages
    ]

    fields: dict = {"namespace_id": str(namespace)}
    if group_id is not None:
        fields["group_id"] = group_id
    normalizer = MetadataFieldNormalizer(fields=fields)
    chunk_docs = await normalizer.anormalize(chunk_docs)
    page_docs = await normalizer.anormalize(page_docs)

    return await pipeline.aprocess(PagedInput(pages=page_docs, chunks=chunk_docs))
