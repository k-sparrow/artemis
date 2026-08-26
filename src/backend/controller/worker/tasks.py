"""Celery tasks for the Artemis ingestion pipeline.

Chain structure
---------------
The Kafka RabbitMQ Sink Connector calls ``tasks.ingest``, which resolves the
namespace UUID and dispatches ``tasks.parse`` standalone (NOT chained with
``index`` — see below for why).

``parse`` replaces itself via ``self.replace`` with a short async sub-chain:

    submit_parse  →  poll_parse

``submit_parse`` writes a ``parse_stage_state`` claim row (see
``backend/claims.py``) BEFORE calling docling-serve, then submits the
conversion job with an HTTP callback registered
(``POST {parsing service}/v1/parse/callback/{obj_id}``). From here, TWO
independent paths can observe the terminal conversion result:

    (A) poll_parse polling docling-serve's status endpoint (the pre-existing,
        always-on safety net — callback delivery is not reliable), or
    (B) docling-serve's callback → the parsing service → ``tasks.advance_from_callback``

Both call ``claims.try_claim`` on the same row; the winner builds and
dispatches the REST of the pipeline explicitly, exactly once:

    resolve_parse  →  submit_chunk  →  poll_chunk  →  index

This tail is NOT wired as a Celery chain/callback anywhere (a chain's cascade
to the next task is unconditional on success and can't be made to respect the
claim) — it only ever exists as a signature built at claim-resolution time in
``_build_and_dispatch_tail_chain``. Chunking remains a fully decoupled stage
from conversion (Epic 21) — ``resolve_parse`` only downloads the conversion
result and caches it; ``submit_chunk`` is a separate task that submits the
hybrid-chunk job. ``poll_chunk`` calls finalize on success and returns the
artifact ``BlobRef``.

``index`` (index queue)
    1. Receives the artifact ``BlobRef`` from the parse tail chain
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

from celery import Task, chain
from celery.signals import worker_ready
from celery.utils.log import get_task_logger
from opentelemetry import metrics, trace

import pybreaker
from sqlalchemy.exc import DatabaseError, InvalidRequestError
from sqlalchemy.orm.exc import StaleDataError

from src.backend.controller.lib.schemas import (
    BlobRef,
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
from src.backend.controller.worker.backend import claims, outbox
from src.backend.controller.worker.backend.db import SessionLocal, session_cleanup
from src.backend.controller.worker.celery import app
from src.backend.controller.worker.config import settings
from src.backend.controller.worker.dependencies import get_s3_client
from src.backend.controller.worker.exceptions import EmptyObjectError, QueueFullRetry
from src.backend.controller.worker.utils import (
    call_chunk_finalize,
    call_chunk_status,
    call_chunk_submit,
    call_delete_service,
    call_indexing_service,
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

# poll_parse/poll_chunk retry up to 1440 times (~24h ceiling); each retry is a
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

# The only parse_stage_state stage in use today (see backend/claims.py).
_CONVERT_STAGE = "convert"

_DB_TRANSIENT_ERRORS = (DatabaseError, InvalidRequestError, StaleDataError)


def _write_with_local_retry(op, *, contract_task_id: str, max_retries: int = 3) -> None:
    """Run *op* (a callable taking a fresh Session), retrying with a FRESH
    session per attempt on a transient Postgres error.

    Used only for the terminal ingestion_status writes (mark_success/
    mark_failure) — unlike before_start's stage write (which can afford to be
    swallowed outright, since the *next* stage's own write self-heals a
    missed one) a lost terminal write has no later write to correct it, so
    it's worth a small bounded in-process retry before giving up. A Celery-
    level self.retry() isn't available here — on_success/on_failure are
    lifecycle hooks, not task bodies.
    """
    for attempt in range(max_retries):
        try:
            session = SessionLocal()
            with session_cleanup(session):
                op(session)
            return
        except _DB_TRANSIENT_ERRORS:
            if attempt + 1 >= max_retries:
                logger.warning(
                    "ingestion_status=terminal_write_failed_after_retries "
                    "task_id=%s",
                    contract_task_id,
                )
                return


class OutboxTask(Task):
    """Base task that keeps ``ingestion_status`` in sync with this task's own
    lifecycle — see ``backend/outbox.py``.

    ``before_start`` advances ``stage`` to this task's own name;
    ``on_failure`` records a terminal FAILURE. Both recover the contract
    ``task_id`` from this task's own invocation kwargs
    (``kwargs.get("task_id")``) — every task in the chain already carries it
    for exactly this reason. ``advance_from_callback`` is the one exception:
    it has no ``task_id`` in its own kwargs (dispatched directly from the
    callback endpoint with only ``obj_id``/``stage``/``docling_task_id``/
    ``outcome``/``error_message``), so both hooks silently no-op for it —
    its failure branch calls ``record_failure`` explicitly instead, once it
    has recovered the real contract id from the claimed ``parse_stage_state``
    row's ``resume_context``. Its (very brief) missing before_start stage
    update is accepted — the very next task in the tail chain's own
    before_start corrects it moments later.

    ``before_start`` fires on EVERY delivery, including every retry — gated
    on ``self.request.retries == 0`` so ``poll_parse``/``poll_chunk``'s
    up-to-1440-retry budget doesn't turn into 1440 Postgres writes per
    document. Its own write failures are swallowed + logged: purely
    observational, and the next stage's own before_start self-heals a missed
    one.
    """

    def before_start(self, task_id, args, kwargs):
        contract_task_id = kwargs.get("task_id")
        if not contract_task_id:
            return
        if self.request.retries != 0:
            return
        try:
            session = SessionLocal()
            with session_cleanup(session):
                outbox.update_stage(session, task_id=contract_task_id, stage=self.name)
        except _DB_TRANSIENT_ERRORS:
            logger.warning(
                "ingestion_status=stage_write_failed task_id=%s stage=%s",
                contract_task_id,
                self.name,
            )

    def record_failure(self, *, contract_task_id: str, failure_reason: str) -> None:
        """Mark ``ingestion_status`` FAILURE for *contract_task_id*.

        Shared by ``on_failure`` (identity recovered from this task's own
        invocation kwargs, above) and ``advance_from_callback`` (identity
        recovered from a claimed ``parse_stage_state`` row's
        ``resume_context`` instead — it has no chain kwargs of its own to
        fall back on).
        """
        _write_with_local_retry(
            lambda session: outbox.mark_failure(
                session, task_id=contract_task_id, failure_reason=failure_reason
            ),
            contract_task_id=contract_task_id,
        )
        logger.warning(
            "task_failure=recorded task=%s contract_task_id=%s",
            self.name,
            contract_task_id,
        )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        contract_task_id = kwargs.get("task_id")
        if not contract_task_id:
            return
        self.record_failure(
            contract_task_id=contract_task_id,
            failure_reason=f"{type(exc).__name__}: {exc}",
        )


class TerminalOutboxTask(OutboxTask):
    """Adds ``on_success`` → mark ``ingestion_status`` SUCCESS. Used only by
    the terminal task of each chain (``index``, ``delete_document``) —
    applying it to every task would flip the row to "success" the moment
    ``submit_parse`` finishes; chain progression itself already implies the
    prior stage succeeded (the next stage's ``before_start`` write supersedes
    it).
    """

    def on_success(self, retval, task_id, args, kwargs):
        contract_task_id = kwargs.get("task_id")
        if not contract_task_id:
            return
        _write_with_local_retry(
            lambda session: outbox.mark_success(session, task_id=contract_task_id),
            contract_task_id=contract_task_id,
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
# parse_stage_state claim helpers — shared by poll_parse and
# advance_from_callback, the two independent paths that can each observe a
# terminal conversion outcome for the same document (docling-serve's HTTP
# callback vs. poll_parse's own polling). See backend/claims.py for the
# atomic UPDATE ... WHERE claimed_at IS NULL RETURNING that arbitrates them.
# ---------------------------------------------------------------------------


def _retry_on_db_error(self, op, *, max_retries: int):
    """Run *op* (a zero-arg callable), retrying via self.retry on a transient
    Postgres error. *max_retries* must match the CALLING task's own retry
    budget (poll_parse: 1440, same as its polling budget — advance_from_callback:
    20, a fresh one-shot task) — self.request.retries is a single counter shared
    across every self.retry() call in a task's lifetime, so passing a smaller
    max_retries than a task may have already reached (e.g. poll_parse mid-way
    through its 1440-retry polling budget) would make Celery raise
    MaxRetriesExceededError immediately instead of scheduling a genuine retry.
    """
    try:
        return op()
    except _DB_TRANSIENT_ERRORS as exc:
        backoff = min(120, 2**self.request.retries)
        raise self.retry(
            exc=exc,
            countdown=backoff + random.uniform(0, backoff),
            max_retries=max_retries,
        )


def _backfill_docling_task_id(
    self, *, obj_id: str, stage: str, docling_task_id: str, max_retries: int
) -> None:
    def _op() -> None:
        session = SessionLocal()
        with session_cleanup(session):
            claims.backfill_docling_task_id(
                session, obj_id=obj_id, stage=stage, docling_task_id=docling_task_id
            )

    _retry_on_db_error(self, _op, max_retries=max_retries)


def _try_claim(
    self, *, obj_id: str, stage: str, max_retries: int
) -> claims.ResumeContext | None:
    """Returns the claimed row's ResumeContext, or None if not won.

    Deliberately returns the validated model rather than a
    claims.ParseStageState ORM instance — see claims.try_claim's own
    docstring for why (an ORM instance returned here would be bound to a
    session this function's own session_cleanup already closed by the time
    the caller touches it).
    """

    def _op() -> claims.ResumeContext | None:
        session = SessionLocal()
        with session_cleanup(session):
            return claims.try_claim(
                session, obj_id=obj_id, stage=stage, claimant=self.request.id
            )

    return _retry_on_db_error(self, _op, max_retries=max_retries)


def _delete_claim_row(*, obj_id: str, stage: str) -> None:
    """Delete the claim row after successfully acting on it.

    Swallows transient DB errors rather than propagating them — by this point
    the real work (tail-chain dispatch or failure record) already succeeded,
    so a cleanup hiccup must never flip that outcome to FAILURE. A row left
    behind here is caught by the periodic prune job instead.
    """
    try:
        session = SessionLocal()
        with session_cleanup(session):
            claims.delete_stage_row(session, obj_id=obj_id, stage=stage)
    except _DB_TRANSIENT_ERRORS:
        logger.warning(
            "parse_stage_claim=delete_failed obj_id=%s stage=%s "
            "(will be swept by the periodic prune job)",
            obj_id,
            stage,
        )


def _build_and_dispatch_tail_chain(
    submit_result: dict, resume_context: claims.ResumeContext
) -> None:
    """Build resolve_parse → submit_chunk → poll_chunk → index and dispatch it.

    Only ever called by whichever of poll_parse / advance_from_callback wins
    the parse_stage_state claim — never linked automatically via a Celery
    chain/callback (see parse()'s docstring for why).
    """
    namespace_id = resume_context.namespace_id
    group_id = resume_context.group_id
    task_id = resume_context.task_id
    operation = resume_context.operation
    # Back to plain JSON-safe dicts here: these cross the broker as task
    # kwargs (Celery's json serializer doesn't know how to encode a Pydantic
    # model), matching every other call site in this file.
    source = resume_context.source.model_dump(mode="json")
    s3 = resume_context.s3.model_dump(mode="json")

    chain(
        resolve_parse.s(
            submit_result,
            namespace_id=namespace_id,
            group_id=group_id,
            task_id=task_id,
            operation=operation,
        ),
        submit_chunk.s(
            namespace_id=namespace_id,
            group_id=group_id,
            task_id=task_id,
            operation=operation,
        ),
        poll_chunk.s(
            source=source,
            namespace_id=namespace_id,
            group_id=group_id,
            task_id=task_id,
            operation=operation,
        ),
        index.s(
            namespace_id=namespace_id,
            upload_action=operation,
            group_id=group_id,
            source=source,
            s3=s3,
            task_id=task_id,
        ),
    ).apply_async()


# ---------------------------------------------------------------------------
# Entry point — called by the Kafka HTTP Sink
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.ingest",
    bind=True,
    pydantic=True,
)
def ingest(
    self,
    s3: S3Details,
    source: SourceDetails,
    upload_action: UploadAction,
    info: IngestionInfo,
) -> dict:
    """Create this run's ingestion_status row, then dispatch the parse →
    index chain for a single document.

    Touches infrastructure directly (unlike a purely routing dispatcher) for
    exactly one reason: this is the only point in the whole pipeline that
    has every identity field (namespace_id, obj_id, source, group_id, …) in
    hand at once, so it's the sole INSERT for the whole run — every later
    task only ever UPDATEs this same row (see backend/outbox.py,
    OutboxTask in this file).
    """
    namespace_id = info.namespace_id

    # This entry task's Celery id is the contract task_id: the AMQP message id
    # the RabbitMQ sink connector injected, which equals the artemis.task_id the
    # storage service stamped on its upload span. Stamp it here and propagate it
    # down the chain so the whole worker pipeline is searchable by that one key.
    # (Each subtask still has its own distinct self.request.id — we only carry
    # this value as the artemis.task_id span tag / ingestion_status PK.)
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
    # OutboxTask.on_failure can recover it uniformly from the task kwargs
    # to build a CDC-readable FAILURE row keyed by the contract task_id.
    # Over the broker `upload_action` arrives as a plain str (celery's pydantic
    # coercion doesn't rebuild the enum), so normalize to its string value.
    operation = getattr(upload_action, "value", upload_action)

    def _write_status_row() -> None:
        session = SessionLocal()
        with session_cleanup(session):
            outbox.create_status_row(
                session,
                task_id=task_id,
                namespace_id=namespace_id,
                obj_id=source.obj_id,
                source=source.source,
                object_type=source.object_type,
                content_type=source.content_type,
                size_bytes=s3.size,
                group_id=info.group_id,
                operation=operation,
            )

    try:
        _write_status_row()
    except _DB_TRANSIENT_ERRORS as exc:
        backoff = min(120, 2**self.request.retries)
        raise self.retry(
            exc=exc,
            countdown=backoff + random.uniform(0, backoff),
            max_retries=20,
        )

    match upload_action:
        case UploadAction.CREATE | UploadAction.UPDATE:
            # `index` is NOT chained here — it's dispatched, along with the rest
            # of the tail (resolve_parse -> submit_chunk -> poll_chunk -> index),
            # by whichever of poll_parse / advance_from_callback wins the
            # parse_stage_state claim for this document (see parse()'s
            # docstring). Chaining it here would auto-fire it via
            # self.replace()'s callback inheritance regardless of that claim.
            result = parse.apply_async(
                args=(s3.model_dump(),),
                kwargs={
                    "source": source.model_dump(mode="json"),
                    "namespace_id": str(namespace_id),
                    "group_id": group_id,
                    "task_id": task_id,
                    "operation": operation,
                },
            )
            return {"task_id": str(result.id)}

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
# Async parse sub-chain — tasks.parse + five subtasks
# ---------------------------------------------------------------------------
#
# Dispatch structure (via tasks.parse → self.replace):
#
#   parse.s(s3, source=…, task_id=…, …)
#     └─ self.replace(chain(
#           submit_parse.s(…)   → SubmitResult dict
#           poll_parse.s(…)     → SubmitResult dict      (polls until conversion done)
#           resolve_parse.s(…)  → ResolveResult dict     (downloads + caches conversion)
#           submit_chunk.s(…)   → ChunkSubmitResult dict (Epic 21 —
#                                                         chunking is its own stage)
#           poll_chunk.s(…)     → BlobRef dict           (polls until chunk done,
#                                                         finalizes)
#        ))
#
# The BlobRef dict from poll_chunk flows into index as its first positional arg.
# The contract task_id from ingest is threaded through all six tasks as a kwarg
# so OutboxTask.on_failure can always key the CDC FAILURE row correctly.


@app.task(
    name="tasks.parse",
    base=OutboxTask,
    bind=True,
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

    Uses self.replace() so Celery substitutes the internal sub-chain in
    place — but only submit_parse -> poll_parse are chained here. Unlike
    before the docling-serve conversion-callback feature, this does NOT
    include resolve_parse/submit_chunk/poll_chunk/index: a Celery chain's
    cascade to the next task is unconditional on success, and can't be made
    to respect the parse_stage_state claim that arbitrates between poll_parse
    and the callback path (see backend/claims.py) — whichever of the two
    observes the terminal outcome AND wins the claim builds and dispatches
    that whole tail explicitly (_build_and_dispatch_tail_chain), exactly
    once, itself. All sub-tasks still receive task_id so
    OutboxTask.on_failure can recover the contract id for CDC
    records regardless of which sub-task fails.
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
                obj_id=source["obj_id"],
                namespace_id=namespace_id,
                group_id=group_id,
                task_id=task_id,
                operation=operation,
            ),
        )
    )


@app.task(
    name="tasks.submit_parse",
    base=OutboxTask,
    bind=True,
    pydantic=True,
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

        obj_id = str(source.obj_id)
        # Written BEFORE the docling-serve call, not after: a very fast
        # document's callback could otherwise fire before this row exists.
        # `mode` is always "source" from this task's own client (call_parse_submit
        # always sends source_ref, never an inline file upload) — a worker-side
        # constant, not something only docling-serve's response would reveal.
        resume_context = claims.ResumeContext(
            namespace_id=str(namespace_id),
            group_id=group_id,
            task_id=task_id,
            operation=operation,
            mode="source",
            source=source,
            s3=s3,
        )

        def _write_claim_row() -> None:
            session = SessionLocal()
            with session_cleanup(session):
                claims.ensure_stage_row(
                    session,
                    obj_id=obj_id,
                    stage=_CONVERT_STAGE,
                    resume_context=resume_context,
                )

        try:
            _write_claim_row()
        except (DatabaseError, InvalidRequestError, StaleDataError) as exc:
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=20,
            )

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
            if exc.response.status_code == 429:
                # docling-serve's Ray task queue is full — expected
                # backpressure, not a failure. Flat delay (no exponential
                # growth, so a job's wait never compounds past a fresher
                # job's) and a 24h-ceiling budget matching
                # DOCLING_SERVE_ENG_RAY_TASK_TIMEOUT — waiting in line should
                # eventually resolve into a running task.
                countdown = 300 + random.uniform(0, 30)
                logger.info(
                    "backpressure=docling_queue_full task=submit_parse "
                    "obj_id=%s retry_in=%.0fs",
                    obj_id,
                    countdown,
                )
                raise self.retry(
                    exc=QueueFullRetry(
                        str(exc), request=exc.request, response=exc.response
                    ),
                    countdown=countdown,
                    max_retries=288,
                )
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
    base=OutboxTask,
    bind=True,
    acks_late=True,
    # Up to 1440 retries/document — a result backend configured (the stock
    # recipe now covers every other task) would call backend.mark_as_retry()
    # on EVERY retry (celery/app/trace.py: store_errors = not eager whenever
    # ignore_result is unset), reintroducing exactly the per-poll Postgres
    # chatter this feature eliminates. ingestion_status (OutboxTask, gated on
    # retries==0) already covers this task's status visibility regardless.
    ignore_result=True,
)
def poll_parse(
    self,
    submit_result: dict,
    *,
    obj_id: str,
    namespace_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Poll conversion status until terminal; claim + advance on a terminal result.

    Retries with a 60–75 s countdown while status is "processing".
    Transient HTTP errors (5xx, breaker) draw from the same max_retries=1440
    budget as in-progress polls (24 h ceiling at 60 s intervals).

    Races docling-serve's HTTP callback (advance_from_callback) for a terminal
    result on the same document — both call claims.try_claim on the same
    parse_stage_state row; whichever wins dispatches the tail chain (success)
    or records the failure (failure), the other is a silent no-op. This is the
    safety net: callback delivery is not reliable (docling-serve retries 3x
    over ~1-2s then silently drops it), so this poll loop still runs
    regardless of whether the callback ever fires.

    acks_late=True: between retries this task is "parked" in the worker's
    in-memory timer waiting for its ETA, not sitting acked-and-forgotten in
    RabbitMQ. Without acks_late, a worker crash during that park window drops
    the message permanently (RabbitMQ already considers it delivered). With
    acks_late, the crash leaves it unacked and RabbitMQ redelivers it.
    """
    parsing_task_id = submit_result["parsing_task_id"]
    # obj_id comes from this task's own kwarg (threaded from parse(), which
    # got it from source — the worker's own already-known identity), NOT
    # from submit_result["obj_id"] — that value is only the parsing
    # service's OWN echo of what it received, an external round-trip this
    # code shouldn't trust as a claims.py primary key.

    # Belt-and-suspenders: submit_parse already wrote docling_task_id=None
    # before its HTTP call. Backfilling here is purely observational (the
    # callback path never reads this column) — same 1440-retry budget as the
    # rest of this task on a transient DB error (see _retry_on_db_error).
    _backfill_docling_task_id(
        self,
        obj_id=obj_id,
        stage=_CONVERT_STAGE,
        docling_task_id=parsing_task_id,
        max_retries=1440,
    )

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
        claimed = _try_claim(
            self, obj_id=obj_id, stage=_CONVERT_STAGE, max_retries=1440
        )
        if claimed is None:
            # Lost the claim: the callback path already handled (or is about
            # to handle) this outcome. Raising here too would double-record
            # the failure (a second, separately-keyed FAILURE row via this
            # task's own on_failure, since poll_parse's kwargs already carry
            # task_id/namespace_id) — silently no-op instead.
            return None
        _delete_claim_row(obj_id=obj_id, stage=_CONVERT_STAGE)
        # Won the claim: raise as before so this task's own on_failure fires
        # (unchanged from before this feature — poll_parse's kwargs already
        # carry everything on_failure needs).
        raise DocumentConversionError(
            f"docling-serve conversion failed for task {parsing_task_id}: "
            f"{status.get('error_message', 'no detail')}"
        )

    _poll_attempts.add(1, {"poll_task": "poll_parse", "outcome": "success"})
    elapsed = _elapsed()
    if elapsed is not None:
        _poll_elapsed.record(elapsed, {"poll_task": "poll_parse", "outcome": "success"})

    resume_context = _try_claim(
        self, obj_id=obj_id, stage=_CONVERT_STAGE, max_retries=1440
    )
    if resume_context is None:
        # Lost the claim: the callback path already dispatched the tail chain.
        return None
    _build_and_dispatch_tail_chain(submit_result, resume_context)
    _delete_claim_row(obj_id=obj_id, stage=_CONVERT_STAGE)
    return submit_result


@app.task(
    name="tasks.advance_from_callback",
    base=OutboxTask,
    bind=True,
    acks_late=True,
)
def advance_from_callback(
    self,
    obj_id: str,
    *,
    stage: str,
    docling_task_id: str,
    outcome: str,
    error_message: str | None = None,
) -> dict:
    """Advance the parse pipeline from docling-serve's HTTP callback.

    Dispatched by the parsing service's callback endpoint the instant
    docling-serve reports a terminal conversion result — the fast path that
    lets poll_parse stop being the sole mechanism for detecting completion.
    Races poll_parse for the same parse_stage_state claim; the loser is a
    silent no-op (the other path already handled it, or will).

    Unlike poll_parse, this task has no chain kwargs of its own to recover
    namespace_id/task_id/etc from on failure — that's why the failure branch
    calls self.record_failure(...) explicitly, using the claimed row's
    resume_context, instead of relying on OutboxTask.on_failure's
    automatic kwargs-based recovery (which would silently no-op here).

    DB errors during the claim itself use a short retry budget
    (max_retries=20, a fresh one-shot task — not poll_parse's 1440/24h
    polling budget): this task doesn't need to survive a long Postgres outage
    on its own, poll_parse is independently polling regardless of what
    happens here. If the claim's retries are exhausted, this task fails as a
    plain Celery task WITHOUT calling record_failure — a DB outage means "we
    don't know what happened to this document", not "conversion failed";
    poll_parse remains the authoritative source of truth for the real outcome
    either way.
    """
    resume_context = _try_claim(self, obj_id=obj_id, stage=stage, max_retries=20)
    if resume_context is None:
        return {"status": "not_claimed"}

    if outcome == "success":
        submit_result = {
            "parsing_task_id": docling_task_id,
            "obj_id": obj_id,
            "mode": resume_context.mode,
        }
        _build_and_dispatch_tail_chain(submit_result, resume_context)
        _delete_claim_row(obj_id=obj_id, stage=stage)
        logger.info("advance_from_callback=dispatched obj_id=%s", obj_id)
        return {"status": "dispatched"}

    failure_reason = (
        f"docling-serve conversion failed (via callback) for task "
        f"{docling_task_id}: {error_message or 'no detail'}"
    )
    self.record_failure(
        contract_task_id=resume_context.task_id,
        failure_reason=failure_reason,
    )
    _delete_claim_row(obj_id=obj_id, stage=stage)
    raise DocumentConversionError(failure_reason)


@app.task(
    name="tasks.resolve_parse",
    base=OutboxTask,
    bind=True,
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
    """Download conversion result and cache it (replay + derived pages).

    Calls POST /v1/parse/resolve and returns ResolveResult dict immediately.
    Chunking is a fully separate stage from here (Epic 21, see submit_chunk)
    — this task no longer submits a chunk job itself.
    """
    try:
        return call_parse_resolve(
            submit_result=submit_result,
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_RESOLVE_TIMEOUT,
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


@app.task(
    name="tasks.submit_chunk",
    base=OutboxTask,
    bind=True,
    acks_late=True,
)
def submit_chunk(
    self,
    resolve_result: dict,
    *,
    namespace_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    operation: str | None = None,
) -> dict:
    """Submit the replay-cached DoclingDocument for hybrid chunking.

    Calls POST /v1/chunk/submit and returns ChunkSubmitResult dict
    immediately. A fully separate stage from resolve_parse (Epic 21) —
    docling-serve fetches the replay cache directly from S3, no document
    bytes ever pass through this task.
    """
    try:
        result = call_chunk_submit(
            resolve_result=resolve_result,
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_CHUNK_SUBMIT_TIMEOUT,
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
        if exc.response.status_code == 429:
            # Same reasoning as submit_parse above — queue-full backpressure,
            # not a failure. Flat 24h-ceiling budget, no exponential growth.
            countdown = 300 + random.uniform(0, 30)
            logger.info(
                "backpressure=docling_queue_full task=submit_chunk "
                "task_id=%s retry_in=%.0fs",
                task_id,
                countdown,
            )
            raise self.retry(
                exc=QueueFullRetry(
                    str(exc), request=exc.request, response=exc.response
                ),
                countdown=countdown,
                max_retries=288,
            )
        if exc.response.status_code >= 500:
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=20,
            )
        raise


@app.task(
    name="tasks.poll_chunk",
    base=OutboxTask,
    bind=True,
    acks_late=True,
    # Same reasoning as poll_parse above — shares the 1440-retry budget.
    ignore_result=True,
)
def poll_chunk(
    self,
    chunk_submit_result: dict,
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
    chunking_task_id = chunk_submit_result["chunking_task_id"]
    obj_id = chunk_submit_result["obj_id"]

    def _elapsed() -> float | None:
        submitted_at = chunk_submit_result.get("submitted_at")
        return time.time() - submitted_at if submitted_at is not None else None

    try:
        status = call_chunk_status(
            task_id=chunking_task_id,
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_STATUS_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        _poll_attempts.add(
            1, {"poll_task": "poll_chunk", "outcome": "circuit_breaker_retry"}
        )
        jitter = random.uniform(0, parsing_breaker.reset_timeout * 0.25)
        raise self.retry(
            exc=exc,
            countdown=parsing_breaker.reset_timeout + 5 + jitter,
            max_retries=1440,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            _poll_attempts.add(1, {"poll_task": "poll_chunk", "outcome": "5xx_retry"})
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=1440,
            )
        _poll_attempts.add(1, {"poll_task": "poll_chunk", "outcome": "4xx_error"})
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_chunk", "outcome": "4xx_error"}
            )
        raise

    if status["status"] == "processing":
        _poll_attempts.add(1, {"poll_task": "poll_chunk", "outcome": "processing"})
        raise self.retry(
            countdown=60 + random.uniform(0, 15),
            max_retries=1440,
        )
    if status["status"] == "failure":
        _poll_attempts.add(1, {"poll_task": "poll_chunk", "outcome": "failure"})
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_chunk", "outcome": "failure"}
            )
        raise DocumentChunkingError(
            f"docling-serve chunking failed for task {chunking_task_id}: "
            f"{status.get('error_message', 'no detail')}"
        )

    try:
        blob_ref = call_chunk_finalize(
            chunk_submit_result=chunk_submit_result,
            metadata={"obj_id": obj_id},
            parsing_url=settings.PARSING_SERVICE_URL,
            timeout=settings.PARSING_FINALIZE_TIMEOUT,
            logger=logger,
        )
    except pybreaker.CircuitBreakerError as exc:
        _poll_attempts.add(
            1,
            {"poll_task": "poll_chunk", "outcome": "finalize_circuit_breaker_retry"},
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
                1, {"poll_task": "poll_chunk", "outcome": "finalize_5xx_retry"}
            )
            backoff = min(120, 2**self.request.retries)
            raise self.retry(
                exc=exc,
                countdown=backoff + random.uniform(0, backoff),
                max_retries=1440,
            )
        _poll_attempts.add(
            1, {"poll_task": "poll_chunk", "outcome": "finalize_4xx_error"}
        )
        elapsed = _elapsed()
        if elapsed is not None:
            _poll_elapsed.record(
                elapsed, {"poll_task": "poll_chunk", "outcome": "finalize_4xx_error"}
            )
        raise

    _poll_attempts.add(1, {"poll_task": "poll_chunk", "outcome": "success"})
    elapsed = _elapsed()
    if elapsed is not None:
        _poll_elapsed.record(elapsed, {"poll_task": "poll_chunk", "outcome": "success"})
    logger.info("poll_chunk=finalized obj_id=%s key=%s", obj_id, blob_ref.get("key"))
    return blob_ref


# ---------------------------------------------------------------------------
# Task 2 — load chunks from MinIO + call indexing service + cleanup
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.index",
    base=TerminalOutboxTask,
    bind=True,
    pydantic=True,
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
            # Informational only now — the caller's contract task_id, carried
            # for anyone introspecting this task's own AsyncResult. CDC keying
            # of ingestion_tasks comes from ingestion_status (the outbox),
            # written separately by TerminalOutboxTask.on_success.
            task_id=task_id,
        ).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Deletion task — called by ingest for DELETE / AUTO_DELETE actions
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.delete_document",
    base=TerminalOutboxTask,
    bind=True,
    pydantic=True,
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
    ``ingest`` — ``TerminalOutboxTask.on_success`` uses it to mark the
    matching ``ingestion_status`` row, which the CDC fan-out then keys the
    ``ingestion_tasks`` row by.
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
