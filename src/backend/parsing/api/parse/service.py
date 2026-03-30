import uuid
from typing import List

import httpx
from fastapi import UploadFile
from langchain_core.documents import Document

from src.lib.core.adapters.loaders import LoaderError, LoaderFactory
from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamServiceException,
)
from src.lib.core.ingestion.normalizer.metadata import MetadataFieldNormalizer
from src.lib.core.ingestion.types import ChunkType, ParsedChunk

__all__ = ["a_parse"]

ObjectContent = UploadFile


async def a_parse(
    object: ObjectContent,
    loader_factory: LoaderFactory,
    metadata: dict[str, str],
) -> List[ParsedChunk]:
    content = await object.read()
    try:
        loader = loader_factory(content, object.filename, object.content_type)
        docs = loader.load()
    except LoaderError as exc:
        raise DocumentProcessingException(str(exc)) from exc
    except httpx.HTTPError as exc:
        raise UpstreamServiceException(
            service="document-loader",
            message=f"Failed to parse object '{object.filename}': {exc}",
        ) from exc

    if metadata:
        normalizer = MetadataFieldNormalizer(fields=metadata)
        docs = normalizer.normalize(docs)

    return [_to_parsed_chunk(doc) for doc in docs]


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
