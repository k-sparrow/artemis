import json
import uuid
from typing import List, NamedTuple

from langchain_core.documents import Document
from pydantic import TypeAdapter

from src.backend.parsing.lib.artifact import chunk_items_to_parsed
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.indexer.simple import SimpleIndexer
from src.lib.core.ingestion.types import ChunkType, Page, ParseArtifact, ParsedChunk

__all__ = [
    "encode_artifact",
    "artifact_key",
    "replay_key",
    "pages_key",
    "encode_pages",
    "decode_pages",
    "convert_scratch_prefix",
    "resolved_marker_key",
    "finalized_marker_key",
    "origin_filename_from_head",
    "BuiltArtifact",
    "build_artifact",
]

_pages_adapter = TypeAdapter(List[Page])


def encode_artifact(artifact: ParseArtifact) -> bytes:
    """Serialise the parse artifact to JSON bytes for object storage."""
    return artifact.model_dump_json().encode()


def artifact_key(obj_id: str) -> str:
    """Object-storage key for an object's parse artifact (idempotent per obj_id)."""
    return f"parse/{obj_id}.json"


def replay_key(obj_id: str) -> str:
    """Private replay-cache key for an object's lossless ``DoclingDocument``."""
    return f"replay/{obj_id}.json"


def convert_scratch_prefix(obj_id: str) -> str:
    """Per-object scratch prefix docling-serve's S3Target writes the converted
    JSON under (submit_endpoint's source_ref path only). Read once by
    /v1/parse/resolve via a recursive listing, then deleted — never part of
    the parse->index contract.
    """
    return f"scratch/convert/{obj_id}/"


def pages_key(obj_id: str) -> str:
    """Private cache key for an object's page-level Markdown parents.

    Computed once by /v1/parse/resolve and consumed by /v1/chunk/finalize —
    chunking is a fully decoupled stage (Epic 21), so it never re-derives
    pages from the replay-cached DoclingDocument itself.
    """
    return f"pages/{obj_id}.json"


def resolved_marker_key(obj_id: str) -> str:
    """Key of the marker recording which conversion task /v1/parse/resolve
    last fully cached for this object (its body is that parsing_task_id).

    Makes resolve safe to retry: a repeat call for the same task returns
    immediately instead of re-fetching a result that's already been consumed
    (the scratch object is deleted, docling-serve may have evicted it). Keyed
    on the task id rather than mere presence of the cache so a re-ingestion
    of the same obj_id never short-circuits onto the previous version.
    """
    return f"resolved/{obj_id}"


def finalized_marker_key(obj_id: str) -> str:
    """Key of the marker recording which chunk task /v1/chunk/finalize last
    wrote the parse artifact from (its body is that chunking_task_id). Same
    retry-safety role as :func:`resolved_marker_key`.
    """
    return f"finalized/{obj_id}"


def encode_pages(pages: List[Page]) -> bytes:
    """Serialise page parents to JSON bytes for the private pages cache."""
    return _pages_adapter.dump_json(pages)


def decode_pages(data: bytes) -> List[Page]:
    """Deserialise page parents from the private pages cache."""
    return _pages_adapter.validate_json(data)


def origin_filename_from_head(head: bytes) -> str:
    """``origin.filename`` of a serialised ``DoclingDocument``, read from just
    the first bytes of its JSON.

    ``origin`` is one of the first fields docling writes (before the body and
    text items that make up nearly all of the file), so the leading chunk of
    the object is enough — the point being to label pages with the same
    ``source`` as :func:`build_pages` without downloading and validating a
    document that can be hundreds of MB. Returns "" when ``origin`` is absent,
    null, or not found in *head*, same as ``build_pages`` does for a document
    with no origin.
    """
    text = head.decode("utf-8", errors="ignore")
    marker = '"origin":'
    start = text.find(marker)
    if start == -1:
        return ""
    start += len(marker)
    while start < len(text) and text[start].isspace():
        start += 1
    try:
        origin, _ = json.JSONDecoder().raw_decode(text, start)
    except ValueError:
        return ""
    if not isinstance(origin, dict):
        return ""
    return origin.get("filename") or ""


class BuiltArtifact(NamedTuple):
    data: bytes
    num_pages: int
    num_chunks: int
    fallback_used: bool


def _fallback_split(pages: List[Page], obj_id: uuid.UUID) -> List[ParsedChunk]:
    page_docs = [
        Document(
            page_content=page.markdown,
            metadata={"obj_id": str(obj_id), "page_no": page.page_no},
        )
        for page in pages
    ]
    fallback_docs = SimpleIndexer().process(page_docs)
    if not fallback_docs:
        raise DocumentProcessingException(
            f"chunking produced zero chunks for obj_id={obj_id}, and "
            "the fallback page-split also produced zero chunks "
            "(pages are likely whitespace-only)"
        )
    return [
        ParsedChunk(
            page_content=doc.page_content,
            source="fallback-page-split",
            type=ChunkType.TEXT,
            obj_id=obj_id,
            page_no=doc.metadata.get("page_no"),
        )
        for doc in fallback_docs
    ]


def build_artifact(
    chunk_result: bytes, pages_data: bytes, obj_id: uuid.UUID
) -> BuiltArtifact:
    """Build the serialised ``ParseArtifact`` from docling-serve's raw chunk
    result body and the cached pages (see :func:`encode_pages`).

    Everything CPU-bound in /v1/chunk/finalize lives here — parsing the chunk
    result, mapping it, the zero-chunk fallback split, and encoding the
    artifact — and all of it grows with the document. Plain bytes in and out,
    no I/O, so the endpoint can run it off the event loop in one hop and it
    could move to a process pool unchanged.

    If docling-serve's hybrid chunker produced zero chunks, falls back to a
    plain character-count split of the pages; raises
    ``DocumentProcessingException`` if that also produces nothing.
    """
    chunks = chunk_items_to_parsed(json.loads(chunk_result)["chunks"], obj_id)
    pages = decode_pages(pages_data)
    fallback_used = not chunks
    if fallback_used:
        chunks = _fallback_split(pages, obj_id)
    artifact = ParseArtifact(pages=pages, chunks=chunks)
    return BuiltArtifact(
        data=encode_artifact(artifact),
        num_pages=len(pages),
        num_chunks=len(chunks),
        fallback_used=fallback_used,
    )
