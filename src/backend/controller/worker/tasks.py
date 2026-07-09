"""Celery tasks for the Artemis ingestion pipeline.

Chain structure
---------------
The Kafka RabbitMQ Sink Connector calls ``tasks.ingest``, which resolves the namespace
UUID and dispatches the two-task outer chain:

    parse  →  index

``parse`` replaces itself via ``self.replace`` with the async sub-chain:

    submit_parse  →  poll_parse  →  resolve_parse  →  poll_resolve

``poll_resolve`` calls finalize on success and returns the artifact ``BlobRef``.

``index`` (index queue)
    1. Receives the artifact ``BlobRef`` from the parse sub-chain
    2. POSTs it to the indexing service, which reads the artifact from storage
    3. Deletes the artifact on success (leaves it on failure for replay)
    4. Returns the UpsertResult dict

Raw bytes, chunk lists, and parse artifacts never cross a task boundary or an
inter-service HTTP body — only the small ``BlobRef`` is passed, keeping both the
wire and the Postgres result backend lean.
"""

from __future__ import annotations

import logging
import random
import time
import uuid

import httpx

from celery import Task, chain, states
from celery.signals import worker_ready
from celery.utils.log import get_task_logger
from opentelemetry import metrics, trace

import pybreaker

from src.backend.controller.lib.schemas import (
    BlobRef,
    FailureObject,
    FailureRecord,
    FailureScope,
    IngestionInfo,
    IngestionResult,
    IndexingOutcome,
    ObjectMetadata,
    ObjectProperties,
    ObjectScope,
    S3Details,
    SourceDetails,
    UploadAction,
)
from src.backend.controller.worker.backend.database import DatabaseBackend
from src.backend.controller.worker.celery import app
from src.backend.controller.worker.config import settings
from src.backend.controller.worker.dependencies import get_s3_client
from src.backend.controller.worker.exceptions import EmptyObjectError
from src.backend.controller.worker.utils import (
    call_delete_service,
    call_indexing_service,
    call_parse_finalize,
    call_parse_resolve,
    call_parse_status,
    call_parse_submit,
    indexing_breaker,
    parsing_breaker,
)
from src.lib.core.adapters.stores.minio.blob import MinioBlobStore

logger = get_task_logger(__name__)
logger.setLevel(logging.DEBUG)

_tracer = trace.get_tracer("controller.worker")

# poll_parse/poll_resolve retry up to 1440 times (~24h ceiling); each retry is a
# brand-new task execution, so per-attempt visibility goes to Prometheus counters/
# histograms (cheap, aggregating) rather than spans (one stored record per attempt
# would mean up to 1440 spans per document in the trace backend).
_meter = metrics.get_meter("controller.worker")
_poll_attempts = _meter.create_counter(
    "parse.poll.attempts",
    unit="1",
    description="Poll attempts against docling-serve status, per poll task and outcome",
)
_poll_elapsed = _meter.create_histogram(
    "parse.poll.elapsed_seconds",
    unit="s",
    description="Wall-clock time from submit to terminal poll outcome",
)

# Single shared result-backend instance — avoids recreating the SQLAlchemy
# engine for every task invocation.
_db_backend = DatabaseBackend(
    app=app,
    dburi=settings.BACKEND_RESULT_URL,
    engine_options={"echo": False},
    serializer="json",
)

# Bound on the failure_reason length so a multi-KB traceback message never bloats
# the result column / CDC payload — the full traceback still lives elsewhere.
_FAILURE_REASON_MAX = 2000


class FailureRecordingTask(Task):
    """Base task that records a clean, CDC-readable FAILURE row on terminal failure.

    On a native failure Celery stores the exception encoding
    (``{exc_type, exc_message, exc_module}``) in ``apollo_celery_taskmeta.result``
    — it has no contract ``task_id``/``namespace_id``, so the ksqlDB
    ``ingestion_tasks`` fan-out cannot key or populate a row from it.  This hook
    OVERWRITES this task's own result row with a :class:`FailureRecord` (identity
    recovered from the task kwargs) and nulls the traceback column, so the fan-out
    reads it with the same ``EXTRACTJSONFIELD(result, …)`` paths it uses for SUCCESS.

    Celery's ``handle_failure`` runs ``mark_as_failure`` (the exception write) BEFORE
    ``on_failure``, so this overwrite is the LAST write — no ``Ignore()`` needed (and
    the FAILURE state still short-circuits the chain).  The two writes surface as two
    Debezium events; the fan-out's ``$.task_id IS NOT NULL`` guard drops the
    pre-overwrite (exception-encoded) one.

    Uses ``self.backend`` (the bound per-task ``_db_backend``) — NOT
    ``self.app.AsyncResult``: this app has no app-level backend.
    """

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        contract_task_id = kwargs.get("task_id")
        namespace_id = kwargs.get("namespace_id")
        if not contract_task_id or not namespace_id:
            # Without the contract id + NOT-NULL namespace_id we cannot build a
            # resolvable row — leave the native FAILURE row untouched.
            return

        source = kwargs.get("source") or {}
        obj_id = source.get("obj_id") if isinstance(source, dict) else None
        operation = kwargs.get("operation") or kwargs.get("upload_action") or ""
        operation = getattr(operation, "value", operation)

        payload = FailureRecord(
            task_id=str(contract_task_id),
            object=FailureObject(
                id=obj_id,
                scope=FailureScope(namespace_id=namespace_id),
            ),
            operation=str(operation),
            failure_reason=f"{type(exc).__name__}: {exc}"[:_FAILURE_REASON_MAX],
        ).model_dump(mode="json")

        # Pass request= so the backend preserves the extended columns (name, args,
        # kwargs, worker, …). Our custom DatabaseBackend._update_result writes EVERY
        # column from the result meta, and _get_result_meta only fills the extended
        # columns when a request is given — omitting it NULLs `name`, which would make
        # the ksqlDB FAILURE fan-out's `name IN (…)` filter drop this row.
        self.backend.store_result(
            self.request.id,
            payload,
            state=states.FAILURE,
            traceback=None,
            request=self.request,
        )
        logger.warning(
            "task_failure=recorded task=%s contract_task_id=%s obj_id=%s op=%s",
            self.name,
            contract_task_id,
            obj_id,
            operation,
        )


@worker_ready.connect
def _ensure_parsed_chunks_bucket(**kwargs) -> None:
    """Create the parsed-chunks MinIO bucket when the worker becomes ready.

    Runs once per worker process, after the broker connection is established
    and just before the worker starts consuming tasks.  Using the
    ``worker_ready`` signal (rather than module-level code) avoids triggering
    a MinIO connection during unit-test imports.
    """
    client = get_s3_client()
    if not client.bucket_exists(settings.PARSED_CHUNKS_BUCKET):
        client.make_bucket(settings.PARSED_CHUNKS_BUCKET)
        logger.info(
            "worker_ready=bucket_created bucket=%s", settings.PARSED_CHUNKS_BUCKET
        )


# ---------------------------------------------------------------------------
# Exceptions for permanent parse failures (non-retryable)
# ---------------------------------------------------------------------------


class DocumentConversionError(Exception):
    """Raised when docling-serve reports a terminal failure for a conversion job."""


class DocumentChunkingError(Exception):
    """Raised when docling-serve reports a terminal failure for a chunk job."""


# ---------------------------------------------------------------------------
# Entry point — called by the Kafka HTTP Sink
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.ingest",
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
)
def ingest(
    self,
    s3: S3Details,
    source: SourceDetails,
    upload_action: UploadAction,
    info: IngestionInfo,
) -> dict:
    """Dispatch the parse → index chain for a single document."""
    namespace_id = info.namespace_id

    # This entry task's Celery id is the contract task_id: the AMQP message id
    # the RabbitMQ sink connector injected, which equals the artemis.task_id the
    # storage service stamped on its upload span. Stamp it here and propagate it
    # down the chain so the whole worker pipeline is searchable by that one key.
    # (Each subtask still has its own distinct self.request.id / result-backend
    # PK — we only carry this value as the artemis.task_id span tag.)
    task_id = self.request.id or str(uuid.uuid4())
    trace.get_current_span().set_attribute("artemis.task_id", task_id)

    logger.info(
        "ingest=dispatch action=%s namespace=%s object=%s",
        upload_action,
        namespace_id,
        s3.object,
    )

    group_id = str(info.group_id) if info.group_id is not None else None

    # Identity (namespace_id, source, task_id, operation) is passed by KEYWORD so
    # FailureRecordingTask.on_failure can recover it uniformly from the task kwargs
    # to build a CDC-readable FAILURE row keyed by the contract task_id.
    # Over the broker `upload_action` arrives as a plain str (celery's pydantic
    # coercion doesn't rebuild the enum), so normalize to its string value.
    operation = getattr(upload_action, "value", upload_action)
    match upload_action:
        case UploadAction.CREATE | UploadAction.UPDATE:
            result = chain(
                parse.s(
                    s3.model_dump(),
                    source=source.model_dump(mode="json"),
                    namespace_id=str(namespace_id),
                    group_id=group_id,
                    task_id=task_id,
                    operation=operation,
                ),
                index.s(
                    namespace_id=str(namespace_id),
                    upload_action=operation,
                    group_id=group_id,
                    source=source.model_dump(mode="json"),
                    s3=s3.model_dump(mode="json"),
                    task_id=task_id,
                ),
            ).apply_async()
            return {"chain_id": str(result.id)}

        case UploadAction.DELETE | UploadAction.AUTO_DELETE:
            result = delete_document.apply_async(
                kwargs={
                    "source": source.model_dump(),
                    "namespace_id": str(namespace_id),
                    "task_id": task_id,
                    "operation": operation,
                }
            )
            return {"task_id": str(result.id)}


# ---------------------------------------------------------------------------
# Async parse sub-chain — tasks.parse + four polling subtasks
# ---------------------------------------------------------------------------
#
# Dispatch structure (via tasks.parse → self.replace):
#
#   parse.s(s3, source=…, task_id=…, …)
#     └─ self.replace(chain(
#           submit_parse.s(…)   → SubmitResult dict
#           poll_parse.s(…)     → SubmitResult dict (polls until conversion done)
#           resolve_parse.s(…)  → ResolveResult dict
#           poll_resolve.s(…)   → BlobRef dict      (polls until chunk done)
#        ))
#
# The BlobRef dict from poll_resolve flows into index as its first positional arg.
# The contract task_id from ingest is threaded through all five tasks as a kwarg
# so FailureRecordingTask.on_failure can always key the CDC FAILURE row correctly.


@app.task(
    name="tasks.parse",
    base=FailureRecordingTask,
    bind=True,
    backend=_db_backend,
    result_serializer="json",
    acks_late=True,
)
def parse(
    self,
    s3: dict,
    *,
    source: dict,
    namespace_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> None:
    """Dispatch point for the async parse sub-chain.

    Uses self.replace() so Celery substitutes the internal sub-chain in place
    and threads its final BlobRef result directly into index. All sub-tasks
    receive task_id so FailureRecordingTask.on_failure can recover the contract
    id for CDC records regardless of which sub-task fails.
    """
    return self.replace(
        chain(
            submit_parse.s(
                s3,
                source=source,
                namespace_id=namespace_id,
                group_id=group_id,
                task_id=task_id,
                operation=operation,
            ),
            poll_parse.s(
                namespace_id=namespace_id,
                group_id=group_id,
                task_id=task_id,
                operation=operation,
            ),
            resolve_parse.s(
                namespace_id=namespace_id,
                group_id=group_id,
                task_id=task_id,
                operation=operation,
            ),
            poll_resolve.s(
                source=source,
                namespace_id=namespace_id,
                group_id=group_id,
                task_id=task_id,
                operation=operation,
            ),
        )
    )


@app.task(
    name="tasks.submit_parse",
    base=FailureRecordingTask,
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    acks_late=True,
)
def submit_parse(
    self,
    s3: S3Details,
    *,
    source: SourceDetails,
    namespace_id: uuid.UUID,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Fetch source from MinIO via claim-check and queue a conversion job.

    Submits large PDFs as a batch S3 conversion; all other documents as a
    single async file conversion. Returns SubmitResult dict immediately.
    """
    with _tracer.start_as_current_span("tasks.submit_parse") as span:
        task_id = task_id or self.request.id or str(uuid.uuid4())
        span.set_attribute("artemis.task_id", task_id)
        span.set_attribute("artemis.namespace_id", str(namespace_id))
        span.set_attribute("artemis.obj_id", str(source.obj_id))

        if s3.size == 0:
            raise EmptyObjectError(str(source.obj_id))

        source_ref = BlobRef(bucket=s3.bucket, key=s3.object)
        try:
            result = call_parse_submit(
                source_ref=source_ref,
                source=source,
                parsing_url=settings.PARSING_SERVICE_URL,
                timeout=settings.PARSING_SUBMIT_TIMEOUT,
                logger=logger,
            )
            result["submitted_at"] = time.time()
            return result
        except pybreaker.CircuitBreakerError as exc:
            jitter = random.uniform(0, parsing_breaker.reset_timeout * 0.25)
            raise self.retry(
                exc=exc,
                countdown=parsing_breaker.reset_timeout + 5 + jitter,
                max_retries=20,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                backoff = min(120, 2**self.request.retries)
                raise self.retry(
                    exc=exc,
                    countdown=backoff + random.uniform(0, backoff),
                    max_retries=20,
                )
            raise


@app.task(
    name="tasks.poll_parse",
    base=FailureRecordingTask,
    bind=True,
    backend=_db_backend,
    result_serializer="json",
    acks_late=True,
)
def poll_parse(
    self,
    submit_result: dict,
    *,
    namespace_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Poll conversion status until terminal; pass SubmitResult through on success.

    Retries with a 60–75 s countdown while status is "processing".
    Transient HTTP errors (5xx, breaker) draw from the same max_retries=1440
    budget as in-progress polls (24 h ceiling at 60 s intervals).

    acks_late=True: between retries this task is "parked" in the worker's
    in-memory timer waiting for its ETA, not sitting acked-and-forgotten in
    RabbitMQ. Without acks_late, a worker crash during that park window drops
    the message permanently (RabbitMQ already considers it delivered). With
    acks_late, the crash leaves it unacked and RabbitMQ redelivers it.
    """
    parsing_task_id = submit_result["parsing_task_id"]

    def _elapsed() -> float | None:
        submitted_at = submit_result.get("submitted_at")
        return time.time() - submitted_at if submitted_at is not None else None

    try:
        status = call_parse_status(
            task_id=parsing_task_id,
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_STATUS_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        _poll_attempts.add(
            1, {"poll_task": "poll_parse", "outcome": "circuit_breaker_retry"}
        )
        jitter = random.uniform(0, parsing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=parsing_breaker.reset_timeout + 5 + jitter,
            max_retries=1440,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            _poll_attempts.add(1, {"poll_task": "poll_parse", "outcome": "5xx_retry"})
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=1440,
            )
        _poll_attempts.add(1, {"poll_task": "poll_parse", "outcome": "4xx_error"})
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_parse", "outcome": "4xx_error"}
            )
        raise

    if status["status"] == "processing":
        _poll_attempts.add(1, {"poll_task": "poll_parse", "outcome": "processing"})
        raise self.retry(
            countdown=60 + random.uniform(0, 15),
            max_retries=1440,
        )
    if status["status"] == "failure":
        _poll_attempts.add(1, {"poll_task": "poll_parse", "outcome": "failure"})
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_parse", "outcome": "failure"}
            )
        raise DocumentConversionError(
            f"docling-serve conversion failed for task {parsing_task_id}: "
            f"{status.get('error_message', 'no detail')}"
        )

    _poll_attempts.add(1, {"poll_task": "poll_parse", "outcome": "success"})
    elapsed = _elapsed()
    if elapsed is not None:
        _poll_elapsed.record(elapsed, {"poll_task": "poll_parse", "outcome": "success"})
    return submit_result


@app.task(
    name="tasks.resolve_parse",
    base=FailureRecordingTask,
    bind=True,
    backend=_db_backend,
    result_serializer="json",
    acks_late=True,
)
def resolve_parse(
    self,
    submit_result: dict,
    *,
    namespace_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Download conversion result, concatenate shards, submit chunk job.

    Calls POST /v1/parse/resolve and returns ResolveResult dict with
    chunking_task_id immediately.
    """
    try:
        result = call_parse_resolve(
            submit_result=submit_result,
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_RESOLVE_TIMEOUT,
            logger=logger,
        )
        result["resolved_at"] = time.time()
        return result
    except pybreaker.CircuitBreakerError as exc:
        jitter = random.uniform(0, parsing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=parsing_breaker.reset_timeout + 5 + jitter,
            max_retries=20,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=20,
            )
        raise


@app.task(
    name="tasks.poll_resolve",
    base=FailureRecordingTask,
    bind=True,
    backend=_db_backend,
    result_serializer="json",
    acks_late=True,
)
def poll_resolve(
    self,
    resolve_result: dict,
    *,
    source: dict,
    namespace_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Poll chunk status until terminal; call finalize on success → BlobRef dict.

    Retries with a 60–75 s countdown while chunking is in progress.
    Both the poll and the finalize call share the same 1440-retry budget.
    """
    chunking_task_id = resolve_result["chunking_task_id"]
    obj_id = resolve_result["obj_id"]

    def _elapsed() -> float | None:
        resolved_at = resolve_result.get("resolved_at")
        return time.time() - resolved_at if resolved_at is not None else None

    try:
        status = call_parse_status(
            task_id=chunking_task_id,
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_STATUS_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        _poll_attempts.add(
            1, {"poll_task": "poll_resolve", "outcome": "circuit_breaker_retry"}
        )
        jitter = random.uniform(0, parsing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=parsing_breaker.reset_timeout + 5 + jitter,
            max_retries=1440,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            _poll_attempts.add(1, {"poll_task": "poll_resolve", "outcome": "5xx_retry"})
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=1440,
            )
        _poll_attempts.add(1, {"poll_task": "poll_resolve", "outcome": "4xx_error"})
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_resolve", "outcome": "4xx_error"}
            )
        raise

    if status["status"] == "processing":
        _poll_attempts.add(1, {"poll_task": "poll_resolve", "outcome": "processing"})
        raise self.retry(
            countdown=60 + random.uniform(0, 15),
            max_retries=1440,
        )
    if status["status"] == "failure":
        _poll_attempts.add(1, {"poll_task": "poll_resolve", "outcome": "failure"})
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_resolve", "outcome": "failure"}
            )
        raise DocumentChunkingError(
            f"docling-serve chunking failed for task {chunking_task_id}: "
            f"{status.get('error_message', 'no detail')}"
        )

    try:
        blob_ref = call_parse_finalize(
            resolve_result=resolve_result,
            metadata={"obj_id": obj_id},
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_FINALIZE_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        _poll_attempts.add(
            1,
            {"poll_task": "poll_resolve", "outcome": "finalize_circuit_breaker_retry"},
        )
        jitter = random.uniform(0, parsing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=parsing_breaker.reset_timeout + 5 + jitter,
            max_retries=1440,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            _poll_attempts.add(
                1, {"poll_task": "poll_resolve", "outcome": "finalize_5xx_retry"}
            )
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=1440,
            )
        _poll_attempts.add(
            1, {"poll_task": "poll_resolve", "outcome": "finalize_4xx_error"}
        )
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_resolve", "outcome": "finalize_4xx_error"}
            )
        raise

    _poll_attempts.add(1, {"poll_task": "poll_resolve", "outcome": "success"})
    elapsed = _elapsed()
    if elapsed is not None:
        _poll_elapsed.record(
            elapsed, {"poll_task": "poll_resolve", "outcome": "success"}
        )
    logger.info("poll_resolve=finalized obj_id=%s key=%s", obj_id, blob_ref.get("key"))
    return blob_ref


# ---------------------------------------------------------------------------
# Task 2 — load chunks from MinIO + call indexing service + cleanup
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.index",
    base=FailureRecordingTask,
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
)
def index(
    self,
    artifact_ref: BlobRef,
    namespace_id: uuid.UUID,
    upload_action: str,
    group_id: str | None = None,
    source: SourceDetails | None = None,
    s3: S3Details | None = None,
    task_id: str | None = None,
) -> dict:
    """Index the parse artifact at *artifact_ref*, then delete it on success.

    Indexing reads the artifact directly from object storage (claim-check); the
    controller only threads the :class:`BlobRef`. On success the artifact is
    removed; on failure the task raises before cleanup, leaving it for
    dead-letter inspection / replay.
    """
    with _tracer.start_as_current_span("tasks.index") as span:
        # Contract task_id propagated from `ingest` via the chain.
        if task_id:
            span.set_attribute("artemis.task_id", task_id)
        span.set_attribute("artemis.namespace_id", str(namespace_id))
        obj_id = str(source.obj_id) if source is not None else ""
        span.set_attribute("artemis.obj_id", obj_id)

        try:
            result = call_indexing_service(
                artifact_ref=artifact_ref,
                namespace_id=namespace_id,
                ingestion_url=settings.INGESTION_SERVICE_URL,
                timeout=settings.HTTPX_TIMEOUT,
                logger=logger,
                group_id=group_id,
            )
        except pybreaker.CircuitBreakerError as exc:
            jitter = random.uniform(0, indexing_breaker.reset_timeout * 0.25)
            raise self.retry(
                exc=exc,
                countdown=indexing_breaker.reset_timeout + 5 + jitter,
                max_retries=20,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                backoff = min(120, 2**self.request.retries)
                raise self.retry(
                    exc=exc,
                    countdown=backoff + random.uniform(0, backoff),
                    max_retries=20,
                )
            raise

        # Cleanup: delete the artifact after a successful index (orchestrator owns
        # the artifact lifecycle). Left in place on failure for replay.
        MinioBlobStore(get_s3_client(), artifact_ref.bucket).delete(artifact_ref.key)
        logger.info("index=cleanup key=%s obj_id=%s", artifact_ref.key, obj_id)

        return IngestionResult(
            object=ObjectMetadata(
                id=uuid.UUID(obj_id),
                source=source.source if source is not None else "",
                scope=ObjectScope(
                    namespace_id=namespace_id,
                    group_id=uuid.UUID(group_id) if group_id is not None else None,
                ),
                properties=ObjectProperties(
                    object_type=source.object_type if source is not None else "",
                    content_type=source.content_type if source is not None else "",
                    size_bytes=s3.size if s3 is not None else None,
                ),
            ),
            indexing=IndexingOutcome(
                num_added=result["num_added"],
                num_skipped=result["num_skipped"],
            ),
            operation=upload_action,
            # Contract task_id (the id the caller holds) → keys ingestion_tasks.
            task_id=task_id,
        ).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Deletion task — called by ingest for DELETE / AUTO_DELETE actions
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.delete_document",
    base=FailureRecordingTask,
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
)
def delete_document(
    self,
    source: SourceDetails,
    namespace_id: uuid.UUID,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Remove a single document from the indexing service.

    Calls ``DELETE /ingest?namespace=<namespace_id>&obj_id=<obj_id>`` on the
    indexing service, which deletes all vectorstore chunks and record-manager
    entries for the object. ``task_id`` is the contract id propagated from
    ``ingest`` so the deletion's ``ingestion_tasks`` row is keyed by it.
    """
    try:
        call_delete_service(
            namespace_id=namespace_id,
            obj_id=str(source.obj_id),
            ingestion_url=settings.INGESTION_SERVICE_URL,
            timeout=settings.HTTPX_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        jitter = random.uniform(0, indexing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=indexing_breaker.reset_timeout + 5 + jitter,
            max_retries=20,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=20,
            )
        raise
    logger.info(
        "delete_document=done namespace=%s obj_id=%s", namespace_id, source.obj_id
    )
    return IngestionResult(
        object=ObjectMetadata(
            id=source.obj_id,
            source=source.source,
            scope=ObjectScope(namespace_id=namespace_id, group_id=None),
            properties=ObjectProperties(
                object_type=source.object_type,
                content_type=source.content_type,
                size_bytes=None,
            ),
        ),
        indexing=IndexingOutcome(num_added=0, num_skipped=0),
        operation=UploadAction.DELETE.value,
        task_id=task_id,
    ).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Namespace deletion task — wipes every document in a namespace
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.delete_namespace",
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
)
def delete_namespace(self, namespace_id: uuid.UUID) -> dict:
    """Remove all documents for *namespace_id* from the indexing service.

    Calls ``DELETE /ingest?namespace=<namespace_id>`` (no ``source`` param),
    which triggers a full namespace wipe via ``pipeline.aprocess([])``.
    """
    try:
        call_delete_service(
            namespace_id=namespace_id,
            ingestion_url=settings.INGESTION_SERVICE_URL,
            timeout=settings.HTTPX_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        jitter = random.uniform(0, indexing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=indexing_breaker.reset_timeout + 5 + jitter,
            max_retries=20,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=20,
            )
        raise
    logger.info("delete_namespace=done namespace=%s", namespace_id)
    return {"status": "deleted_namespace", "namespace_id": str(namespace_id)}
