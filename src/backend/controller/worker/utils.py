"""Utility functions for the ingestion worker tasks.

Each function corresponds to one logical step in the ingestion chain and is
deliberately kept small so it can be unit-tested without spinning up Celery.

Circuit breakers
----------------
``_parsing_breaker`` and ``_indexing_breaker`` are module-level singletons so
their state persists across task invocations within the same worker process.
Both are configured with:

  fail_max=3       — open after 3 consecutive failures
  reset_timeout=120 — attempt recovery (HALF-OPEN) after 120 s

When a breaker is OPEN, ``pybreaker.CircuitBreakerError`` is raised immediately
without making a network call.  The calling tasks catch this and retry with a
125–155 s countdown (reset_timeout + 5 s + jitter), keeping worker threads
available instead of blocking on dead HTTP connections.
"""

from __future__ import annotations

import json
import logging
import uuid
from logging import Logger
from typing import Optional

import httpx
import pybreaker
from opentelemetry import trace as otel_trace
from minio import Minio

from src.backend.controller.lib.schemas import BlobRef, S3Details, SourceDetails

__all__ = [
    "fetch_from_s3",
    "call_parse_submit",
    "call_parse_status",
    "call_parse_resolve",
    "call_parse_finalize",
    "call_indexing_service",
    "call_delete_service",
    "parsing_breaker",
    "indexing_breaker",
]

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
    reset_timeout=120,
    name="parsing",
    listeners=[_LoggingListener()],
)

indexing_breaker = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=120,
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


def call_parse_submit(
    source_ref: BlobRef,
    source: SourceDetails,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """POST /v1/parse/submit → SubmitResult dict.

    Fetches source by claim-check reference and queues an async conversion job
    on docling-serve. Returns immediately with a parsing_task_id.
    """
    url = f"{parsing_url.rstrip('/')}/v1/parse/submit"
    logger.info("parse_submit=request url=%s key=%s", url, source_ref.key)

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(
                url,
                data={
                    "source_ref": source_ref.model_dump_json(),
                    "filename": source.source,
                    "content_type": source.content_type,
                    "metadata": json.dumps({"obj_id": str(source.obj_id)}),
                },
            )
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.parse_submit"):
        result = parsing_breaker.call(_request)
    logger.info("parse_submit=done task_id=%s", result.get("parsing_task_id"))
    return result


def call_parse_status(
    task_id: str,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """GET /v1/parse/status/{task_id} → ParseStatus dict.

    Non-blocking single check. Works for both conversion and chunk task_ids.
    """
    url = f"{parsing_url.rstrip('/')}/v1/parse/status/{task_id}"

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.get(url)
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.parse_status"):
        return parsing_breaker.call(_request)


def call_parse_resolve(
    submit_result: dict,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """POST /v1/parse/resolve → ResolveResult dict.

    Downloads the completed conversion result, writes the replay cache, and
    queues a hybrid chunk job. Returns immediately with a chunking_task_id.
    """
    url = f"{parsing_url.rstrip('/')}/v1/parse/resolve"
    logger.info(
        "parse_resolve=request url=%s task_id=%s",
        url,
        submit_result.get("parsing_task_id"),
    )

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(url, json=submit_result)
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.parse_resolve"):
        result = parsing_breaker.call(_request)
    logger.info("parse_resolve=done chunk_task_id=%s", result.get("chunking_task_id"))
    return result


def call_parse_finalize(
    resolve_result: dict,
    metadata: dict,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """POST /v1/parse/finalize → BlobRef dict.

    Fetches chunk result and builds the ParseArtifact from the replay cache.
    Returns a BlobRef pointing at the written artifact.
    """
    url = f"{parsing_url.rstrip('/')}/v1/parse/finalize"
    logger.info(
        "parse_finalize=request url=%s chunk_task_id=%s",
        url,
        resolve_result.get("chunking_task_id"),
    )
    body = {
        "chunking_task_id": resolve_result["chunking_task_id"],
        "obj_id": resolve_result["obj_id"],
        "metadata": metadata,
    }

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(url, json=body)
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.parse_finalize"):
        result = parsing_breaker.call(_request)
    logger.info("parse_finalize=done key=%s", result.get("key"))
    return result


def call_indexing_service(
    artifact_ref: BlobRef,
    namespace_id: uuid.UUID,
    ingestion_url: str,
    timeout: float,
    logger: Logger,
    group_id: str | None = None,
) -> dict:
    """Ask the indexing service to index the artifact at *artifact_ref*.

    Indexing reads the artifact directly from object storage; only the
    :class:`BlobRef` is sent. Returns the UpsertResult dict.

    Raises ``pybreaker.CircuitBreakerError`` when the indexing circuit is OPEN.
    """
    url = f"{ingestion_url.rstrip('/')}/ingest"
    logger.info(
        "indexing=request url=%s namespace=%s key=%s",
        url,
        namespace_id,
        artifact_ref.key,
    )
    params: dict = {"namespace": str(namespace_id)}
    if group_id is not None:
        params["group_id"] = group_id

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(
                url,
                params=params,
                json={"artifact_ref": artifact_ref.model_dump(mode="json")},
            )
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.indexing_service"):
        result = indexing_breaker.call(_request)
    ids = result.get("ids", [])
    ids_preview = ids[:3] + (["..."] if len(ids) > 3 else [])
    logger.info(
        "indexing=done num_added=%s num_skipped=%s ids=%s",
        result.get("num_added"),
        result.get("num_skipped"),
        ids_preview,
    )
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
