"""Utility functions for the ingestion worker tasks.

Each function corresponds to one logical step in the ingestion chain and is
deliberately kept small so it can be unit-tested without spinning up Celery.

Circuit breakers
----------------
``_parsing_breaker`` and ``_indexing_breaker`` are module-level singletons so
their state persists across task invocations within the same worker process.
Both are configured with:

  fail_max=3      — open after 3 consecutive failures
  reset_timeout=60 — attempt recovery (HALF-OPEN) after 60 s

When a breaker is OPEN, ``pybreaker.CircuitBreakerError`` is raised immediately
without making a network call.  Celery's ``autoretry_for=(Exception,)`` on the
calling tasks will catch this and retry with exponential backoff, keeping worker
threads available instead of blocking on dead HTTP connections.
"""

from __future__ import annotations

import json
import logging
import uuid
from logging import Logger
from typing import List, Optional

import httpx
import pybreaker
from opentelemetry import trace as otel_trace
from minio import Minio
from pydantic import TypeAdapter

from src.backend.controller.lib.schemas import S3Details, SourceDetails
from src.lib.core.ingestion.types import ParsedChunk

__all__ = [
    "fetch_from_s3",
    "call_parsing_service",
    "call_indexing_service",
    "call_delete_service",
    "parsing_breaker",
    "indexing_breaker",
]

_chunks_adapter: TypeAdapter[List[ParsedChunk]] = TypeAdapter(List[ParsedChunk])
_log = logging.getLogger(__name__)
_tracer = otel_trace.get_tracer("controller.worker.utils")


# ---------------------------------------------------------------------------
# Circuit breakers
# ---------------------------------------------------------------------------


class _LoggingListener(pybreaker.CircuitBreakerListener):
    """Logs every circuit state transition at WARNING level."""

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState,
        new_state: pybreaker.CircuitBreakerState,
    ) -> None:
        _log.warning(
            "circuit_breaker=%s state=%s->%s",
            cb.name,
            old_state.name,
            new_state.name,
        )


parsing_breaker = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=60,
    name="parsing",
    listeners=[_LoggingListener()],
)

indexing_breaker = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=60,
    name="indexing",
    listeners=[_LoggingListener()],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    """POST *file_bytes* to the parsing service and return the parsed chunks.

    Raises ``pybreaker.CircuitBreakerError`` when the parsing circuit is OPEN.
    """
    url = f"{parsing_url.rstrip('/')}/v1/parse"
    object_source = source.source
    logger.info("parsing=request url=%s source=%s", url, object_source)

    def _request() -> list:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(
                url,
                files={"file": (object_source, file_bytes, source.content_type)},
                data={"metadata": json.dumps({"obj_id": str(source.obj_id)})},
            )
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.parsing_service"):
        raw = parsing_breaker.call(_request)
    chunks = _chunks_adapter.validate_python(raw)
    logger.info("parsing=done chunks=%d", len(chunks))
    return chunks


def call_indexing_service(
    chunks: List[ParsedChunk],
    namespace_id: uuid.UUID,
    ingestion_url: str,
    timeout: float,
    logger: Logger,
    group_id: str | None = None,
) -> dict:
    """POST *chunks* to the indexing service and return the UpsertResult dict.

    Raises ``pybreaker.CircuitBreakerError`` when the indexing circuit is OPEN.
    """
    url = f"{ingestion_url.rstrip('/')}/ingest"
    logger.info(
        "indexing=request url=%s namespace=%s chunks=%d", url, namespace_id, len(chunks)
    )
    params: dict = {"namespace": str(namespace_id)}
    if group_id is not None:
        params["group_id"] = group_id

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(
                url,
                params=params,
                json=[c.model_dump(mode="json") for c in chunks],
            )
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.indexing_service"):
        result = indexing_breaker.call(_request)
    logger.info("indexing=done result=%s", result)
    return result


def call_delete_service(
    namespace_id: uuid.UUID,
    ingestion_url: str,
    timeout: float,
    logger: Logger,
    obj_id: Optional[str] = None,
) -> None:
    """Send a DELETE /ingest request to the indexing service.

    Raises ``pybreaker.CircuitBreakerError`` when the indexing circuit is OPEN.
    """
    url = f"{ingestion_url.rstrip('/')}/ingest"
    params: dict = {"namespace": str(namespace_id)}
    if obj_id is not None:
        params["obj_id"] = obj_id
    logger.info(
        "delete=request url=%s namespace=%s obj_id=%s", url, namespace_id, obj_id
    )

    def _request() -> None:
        with httpx.Client(timeout=timeout) as http:
            response = http.delete(url, params=params)
            response.raise_for_status()

    indexing_breaker.call(_request)
    logger.info("delete=done namespace=%s obj_id=%s", namespace_id, obj_id)
