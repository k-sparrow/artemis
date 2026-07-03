import uuid
from typing import Literal

from docling.datamodel.document import DoclingDocument
from pydantic import BaseModel

from src.lib.core.adapters.loaders.docling import split_pages
from src.lib.core.ingestion.types import ChunkType, Page, ParseArtifact, ParsedChunk

__all__ = ["ParseStatus", "chunk_items_to_parsed", "build_artifact"]


class ParseStatus(BaseModel):
    """Normalised view of a docling-serve task status.

    The three-value contract hides docling-serve's internal ConversionStatus
    variants ("partial_success", "skipped") from the Celery layer.
    """

    status: Literal["processing", "success", "failure"]
    num_processed: int | None = None
    num_total: int | None = None
    error_message: str | None = None


def chunk_items_to_parsed(items: list[dict], obj_id: uuid.UUID) -> list[ParsedChunk]:
    """Map ChunkedDocumentResultItem dicts → ParsedChunk list.

    ``text`` carries the contextualized chunk text (with structural context).
    ``page_numbers[0]`` is the first page the chunk appears on; ``None`` when
    provenance is absent (non-paginated formats).
    Type defaults to TEXT because ChunkedDocumentResultItem only exposes
    string doc-item references, not label info.
    """
    return [_item_to_parsed(item, obj_id) for item in items]


def _item_to_parsed(item: dict, obj_id: uuid.UUID) -> ParsedChunk:
    page_numbers: list[int] = item.get("page_numbers") or []
    doc_items: list[str] = item.get("doc_items") or []
    # TABLE takes priority for mixed chunks, matching MetaExtractor logic.
    # doc_items are ref strings like "#/tables/0" or "#/texts/1".
    if any("/tables/" in ref for ref in doc_items):
        chunk_type = ChunkType.TABLE
    else:
        chunk_type = ChunkType.TEXT
    return ParsedChunk(
        page_content=item["text"],
        source=item.get("filename", ""),
        type=chunk_type,
        obj_id=obj_id,
        page_no=page_numbers[0] if page_numbers else None,
    )


def build_artifact(
    chunks: list[ParsedChunk],
    dl_doc: DoclingDocument,
    obj_id: uuid.UUID,
) -> ParseArtifact:
    """Assemble ParseArtifact from async-path parsed chunks + replay DoclingDocument."""
    pages = split_pages(dl_doc)
    return ParseArtifact(
        pages=[Page(obj_id=obj_id, page_no=pno, markdown=md) for pno, md in pages],
        chunks=chunks,
    )
