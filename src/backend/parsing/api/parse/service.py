from typing import List

import httpx
from fastapi import UploadFile
from langchain_core.documents import Document

from src.lib.core.adapters.loaders import LoaderFactory
from src.lib.core.ingestion.exceptions import UpstreamServiceException
from src.lib.core.ingestion.types import ChunkType, ParsedChunk

__all__ = ["a_parse"]


async def a_parse(file: UploadFile, loader_factory: LoaderFactory) -> List[ParsedChunk]:
    content = await file.read()
    try:
        loader = loader_factory(content, file.filename, file.content_type)
        docs = loader.load()
    except httpx.HTTPError as exc:
        raise UpstreamServiceException(
            service="document-loader",
            message=f"Failed to parse document '{file.filename}': {exc}",
        ) from exc
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
    )
