import uuid
from typing import Literal

from docling.datamodel.document import DoclingDocument
from pydantic import BaseModel

from src.lib.core.adapters.loaders.docling import split_pages
from src.lib.core.ingestion.types import ChunkType, Page, ParsedChunk

__all__ = ["ParseStatus", "chunk_items_to_parsed", "build_pages"]


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


def build_pages(dl_doc: DoclingDocument, obj_id: uuid.UUID) -> list[Page]:
    """Derive page-level Markdown parents from a converted DoclingDocument.

    Computed once in /v1/parse/resolve, right after conversion, and cached for
    /v1/chunk/finalize to merge with the separately-fetched chunks — chunking
    is a fully decoupled stage from conversion (Epic 21), so this half of the
    final ParseArtifact never needs the DoclingDocument re-fetched or re-parsed.

    ``source`` is read from ``dl_doc.origin.filename`` — the same document
    ChunkedDocumentResultItem.filename (see ``_item_to_parsed`` above) is
    derived from server-side, so pages and chunks agree on ``source`` for a
    given obj_id. ``origin`` is optional on DoclingDocument; falls back to
    "" like ``_item_to_parsed`` does for the chunk side.
    """
    pages = split_pages(dl_doc)
    source = dl_doc.origin.filename if dl_doc.origin else ""
    return [
        Page(obj_id=obj_id, page_no=pno, markdown=md, source=source)
        for pno, md in pages
    ]
