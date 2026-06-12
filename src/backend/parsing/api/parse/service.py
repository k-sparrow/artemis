import uuid
from typing import List, Optional

import httpx
from langchain_core.documents import Document
from pydantic import TypeAdapter

from src.lib.core.adapters.loaders import LoaderError, LoaderFactory
from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamServiceException,
)
from src.lib.core.ingestion.normalizer.metadata import MetadataFieldNormalizer
from src.lib.core.ingestion.types import ChunkType, ParsedChunk

__all__ = ["a_parse", "encode_artifact", "artifact_key"]

_chunks_adapter: TypeAdapter[List[ParsedChunk]] = TypeAdapter(List[ParsedChunk])


async def a_parse(
    content: bytes,
    filename: Optional[str],
    content_type: Optional[str],
    loader_factory: LoaderFactory,
    metadata: dict[str, str],
) -> List[ParsedChunk]:
    """Parse raw *content* into chunks. Input bytes are resolved by the router
    from either an inline upload or a claim-check ``BlobRef``."""
    try:
        loader = loader_factory(content, filename, content_type)
        docs = loader.load()
    except LoaderError as exc:
        raise DocumentProcessingException(str(exc)) from exc
    except httpx.HTTPError as exc:
        raise UpstreamServiceException(
            service="document-loader",
            message=f"Failed to parse object '{filename}': {exc}",
        ) from exc

    if metadata:
        normalizer = MetadataFieldNormalizer(fields=metadata)
        docs = normalizer.normalize(docs)

    return [_to_parsed_chunk(doc) for doc in docs]


def encode_artifact(chunks: List[ParsedChunk]) -> bytes:
    """Serialise the parse artifact (Phase 1: a chunk list) to JSON bytes."""
    return _chunks_adapter.dump_json(chunks)


def artifact_key(obj_id: str) -> str:
    """Object-storage key for an object's parse artifact (idempotent per obj_id)."""
    return f"parse/{obj_id}.json"


def _to_parsed_chunk(doc: Document) -> ParsedChunk:
    metadata = doc.metadata
    raw_type = metadata.get("type", ChunkType.TEXT)
    try:
        chunk_type = ChunkType(raw_type)
    except ValueError:
        chunk_type = ChunkType.UNKNOWN
    return ParsedChunk(
        page_content=doc.page_content,
        source=metadata.get("source", ""),
        type=chunk_type,
        obj_id=uuid.UUID(metadata["obj_id"]),
    )
