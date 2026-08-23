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
    "call_chunk_submit",
    "call_chunk_status",
    "call_chunk_finalize",
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


def _is_queue_backpressure(exc: BaseException) -> bool:
    """docling-serve's 429 (DOCLING_SERVE_ENG_RAY_ENABLE_QUEUE_LIMIT_REJECTION)
    means its Ray task queue is full — expected backpressure, not a system
    failure, so it must not count toward the breaker tripping open. The
    calling tasks (submit_parse/submit_chunk) give it its own generous,
    non-exponential retry policy instead."""
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429


parsing_breaker = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=120,
    name="parsing",
    listeners=[_LoggingListener()],
    exclude=[_is_queue_backpressure],
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

    Non-blocking single check for a conversion task. Use call_chunk_status
    for chunk tasks (Epic 21) — both hit the same underlying docling-serve
    proxy logic (one unified task namespace), just different service paths.
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

    Downloads the completed conversion result and writes the replay cache
    (used both for citation/re-chunk lookups and as the source the chunk
    stage reads directly from S3 — see call_chunk_submit). Returns
    immediately with just the obj_id; chunking is a fully separate stage
    (Epic 21), not bundled into resolve.
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
    logger.info("parse_resolve=done obj_id=%s", result.get("obj_id"))
    return result


def call_chunk_submit(
    resolve_result: dict,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """POST /v1/chunk/submit → ChunkSubmitResult dict.

    Submits the replay-cached DoclingDocument (written by call_parse_resolve)
    for hybrid chunking. docling-serve fetches it directly from S3 — this
    call only ever carries an obj_id, never document bytes. Returns
    immediately with a chunking_task_id.
    """
    url = f"{parsing_url.rstrip('/')}/v1/chunk/submit"
    logger.info(
        "chunk_submit=request url=%s obj_id=%s",
        url,
        resolve_result.get("obj_id"),
    )

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(url, json=resolve_result)
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.chunk_submit"):
        result = parsing_breaker.call(_request)
    logger.info("chunk_submit=done chunk_task_id=%s", result.get("chunking_task_id"))
    return result


def call_chunk_status(
    task_id: str,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """GET /v1/chunk/status/{task_id} → ParseStatus dict.

    Non-blocking single check for a chunk task. Identical proxy logic to
    call_parse_status — docling-serve uses one task namespace regardless of
    task type — hits the dedicated /v1/chunk/ path so polling a chunk task
    doesn't read as polling a parse task (Epic 21).
    """
    url = f"{parsing_url.rstrip('/')}/v1/chunk/status/{task_id}"

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.get(url)
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.chunk_status"):
        return parsing_breaker.call(_request)


def call_chunk_finalize(
    chunk_submit_result: dict,
    metadata: dict,
    parsing_url: str,
    timeout: float,
    logger: Logger,
) -> dict:
    """POST /v1/chunk/finalize → BlobRef dict.

    Fetches the chunk result and merges it with the pages call_parse_resolve
    already cached, writing the final ParseArtifact. Returns a BlobRef
    pointing at the written artifact.
    """
    url = f"{parsing_url.rstrip('/')}/v1/chunk/finalize"
    logger.info(
        "chunk_finalize=request url=%s chunk_task_id=%s",
        url,
        chunk_submit_result.get("chunking_task_id"),
    )
    body = {
        "chunking_task_id": chunk_submit_result["chunking_task_id"],
        "obj_id": chunk_submit_result["obj_id"],
        "metadata": metadata,
    }

    def _request() -> dict:
        with httpx.Client(timeout=timeout) as http:
            response = http.post(url, json=body)
            response.raise_for_status()
            return response.json()

    with _tracer.start_as_current_span("http.chunk_finalize"):
        result = parsing_breaker.call(_request)
    logger.info("chunk_finalize=done key=%s", result.get("key"))
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
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                # httpx.HTTPStatusError's own str() is status/URL only — the
                # indexing service's actual detail message (e.g. why /ingest
                # rejected this artifact) never survives into
                # OutboxTask.on_failure's failure_reason otherwise. Re-raise
                # the same type (not a new exception class) so the 5xx-retry
                # /4xx-permanent branching in the index task, which checks
                # isinstance(exc, httpx.HTTPStatusError) and
                # exc.response.status_code, keeps working unchanged.
                try:
                    detail = response.json().get("detail")
                except Exception:
                    detail = None
                if detail:
                    raise httpx.HTTPStatusError(
                        f"{exc}. detail={detail!r}",
                        request=exc.request,
                        response=exc.response,
                    ) from exc
                raise
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
