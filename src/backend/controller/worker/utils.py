"""Utility functions for the ingestion worker tasks.

Each function corresponds to one logical step in the ingestion chain and is
deliberately kept small so it can be unit-tested without spinning up Celery.
"""

from __future__ import annotations

import uuid
from logging import Logger
from typing import List

import httpx
from minio import Minio
from pydantic import TypeAdapter

from src.backend.controller.lib.schemas import S3Details, SourceDetails
from src.lib.core.ingestion.types import ParsedChunk

__all__ = [
    "fetch_from_s3",
    "call_parsing_service",
    "call_indexing_service",
]

_chunks_adapter: TypeAdapter[List[ParsedChunk]] = TypeAdapter(List[ParsedChunk])


def fetch_from_s3(
    client: Minio,
    s3: S3Details,
    logger: Logger,
) -> bytes:
    """Download an object from MinIO and return its raw bytes."""
    logger.info("s3=fetch bucket=%s object=%s", s3.bucket, s3.object)
    response = client.get_object(s3.bucket, s3.object)
    data = response.read()
    logger.info("s3=fetched bytes=%d", len(data))
    return data


def call_parsing_service(
    file_bytes: bytes,
    source: SourceDetails,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> List[ParsedChunk]:
    """POST *file_bytes* to the parsing service and return the parsed chunks."""
    url = f"{parsing_url.rstrip('/')}/v1/parse"
    filename = source.path or "document"
    logger.info("parsing=request url=%s filename=%s", url, filename)

    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            url,
            files={"file": (filename, file_bytes, source.content_type)},
        )
        response.raise_for_status()

    chunks = _chunks_adapter.validate_python(response.json())
    logger.info("parsing=done chunks=%d", len(chunks))
    return chunks


def call_indexing_service(
    chunks: List[ParsedChunk],
    namespace_id: uuid.UUID,
    ingestion_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """POST *chunks* to the indexing service and return the UpsertResult dict."""
    url = f"{ingestion_url.rstrip('/')}/ingest"
    logger.info(
        "indexing=request url=%s namespace=%s chunks=%d", url, namespace_id, len(chunks)
    )

    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            url,
            params={"namespace": str(namespace_id)},
            json=[c.model_dump() for c in chunks],
        )
        response.raise_for_status()

    result = response.json()
    logger.info("indexing=done result=%s", result)
    return result
