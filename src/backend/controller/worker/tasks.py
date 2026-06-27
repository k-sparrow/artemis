"""Celery tasks for the Artemis ingestion pipeline.

Chain structure
---------------
The Kafka RabbitMQ Sink Connector calls ``tasks.ingest``, which resolves the namespace
UUID and dispatches the two-task chain:

    fetch_and_parse  →  index

``fetch_and_parse`` (gpu_bound queue)
    1. Hands the parsing service a ``BlobRef`` to the input (claim-check — the
       controller never downloads the file or moves bytes)
    2. Parsing reads the input from object storage, writes the artifact, and
       returns the artifact's ``BlobRef``, which this task returns to the chain

``index`` (io_bound queue)
    1. Receives the artifact ``BlobRef`` from the previous task
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
import uuid

import httpx

from celery import Task, chain, states
from celery.signals import worker_ready
from celery.utils.log import get_task_logger
from opentelemetry import trace

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
    call_parsing_service,
    indexing_breaker,
    parsing_breaker,
)
from src.lib.core.adapters.stores.minio.blob import MinioBlobStore

logger = get_task_logger(__name__)
logger.setLevel(logging.DEBUG)

_tracer = trace.get_tracer("controller.worker")

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
    """Dispatch the fetch_and_parse → index chain for a single document."""
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
                fetch_and_parse.s(
                    s3.model_dump(),
                    source=source.model_dump(),
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
# Task 1 — fetch from S3 + call parsing service + save chunks to MinIO
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.fetch_and_parse",
    base=FailureRecordingTask,
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    acks_late=True,
)
def fetch_and_parse(
    self,
    s3: S3Details,
    source: SourceDetails,
    namespace_id: uuid.UUID,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Ask the parsing service to parse the input and return the artifact ref.

    Claim-check: the controller no longer downloads the file or moves bytes — it
    hands parsing a :class:`BlobRef` to the input, and parsing reads the bytes,
    writes the artifact to object storage, and returns the artifact's BlobRef
    (serialised here for the chain → :func:`index`).

    Failure modes (non-retryable):
        EmptyObjectError — contract reports size=0; empty file, nothing to index.

    Retry strategy:
        CircuitBreakerError — parsing service known-down; wait one full reset
        window (reset_timeout + jitter) before probing again, up to max_retries.
        HTTPStatusError 5xx — transient upstream failure; exponential backoff
        with jitter, up to max_retries. 4xx errors are permanent and not retried.
    """
    with _tracer.start_as_current_span("tasks.fetch_and_parse") as span:
        # The contract task_id propagated from `ingest`; fall back to this
        # subtask's own id when invoked directly (e.g. in tests).
        task_id = task_id or self.request.id or str(uuid.uuid4())
        span.set_attribute("artemis.task_id", task_id)
        span.set_attribute("artemis.namespace_id", str(namespace_id))
        span.set_attribute("artemis.obj_id", str(source.obj_id))

        # Empty file — permanent, do not retry. (size comes from the contract;
        # missing/zero-byte inputs surface as a parsing failure → breaker retry.)
        if s3.size == 0:
            raise EmptyObjectError(str(source.obj_id))

        source_ref = BlobRef(bucket=s3.bucket, key=s3.object)
        try:
            artifact = call_parsing_service(
                source_ref=source_ref,
                source=source,
                parsing_url=settings.PARSING_SERVICE_URL,
                timeout=settings.HTTPX_TIMEOUT,
                logger=logger,
            )
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
        logger.info("fetch_and_parse=artifact key=%s", artifact.key)
        return artifact.model_dump(mode="json")


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
                ids=result["ids"],
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
            countdown=indexing_breaker.reset_timeout + jitter,
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
        indexing=IndexingOutcome(num_added=0, num_skipped=0, ids=[]),
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
            countdown=indexing_breaker.reset_timeout + jitter,
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
