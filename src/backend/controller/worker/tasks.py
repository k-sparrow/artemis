"""Celery tasks for the Artemis ingestion pipeline.

Chain structure
---------------
The Kafka RabbitMQ Sink Connector calls ``tasks.ingest``, which resolves the namespace
UUID and dispatches the two-task chain:

    fetch_and_parse  →  index

``fetch_and_parse`` (gpu_bound queue)
    1. Downloads file bytes from MinIO (in-memory — never touches result backend)
    2. POSTs bytes to the parsing service → receives List[ParsedChunk]
    3. Saves chunks to MinIO via ParsedChunkStore → returns the object key

``index`` (io_bound queue)
    1. Receives the MinIO object key from the previous task
    2. Loads List[ParsedChunk] from MinIO
    3. POSTs to the indexing service
    4. Deletes the MinIO object on success (leaves it on failure for replay)
    5. Returns the UpsertResult dict

The raw file bytes and chunk lists never cross a task boundary — only the
short MinIO object key is passed between tasks, keeping the Postgres result
backend lean.
"""

from __future__ import annotations

import logging
import uuid

from celery import chain
from celery.signals import worker_ready
from celery.utils.log import get_task_logger

import pybreaker

from src.backend.controller.lib.schemas import (
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
from src.backend.controller.worker.exceptions import (
    ChunkIntegrityError,
    EmptyObjectError,
    S3IntegrityError,
)
from src.backend.controller.worker.utils import (
    call_delete_service,
    call_indexing_service,
    call_parsing_service,
    fetch_from_s3,
)
from src.lib.core.adapters.stores.minio.parsed_chunks import ParsedChunkStore

logger = get_task_logger(__name__)
logger.setLevel(logging.DEBUG)

# Single shared result-backend instance — avoids recreating the SQLAlchemy
# engine for every task invocation.
_db_backend = DatabaseBackend(
    app=app,
    dburi=settings.BACKEND_RESULT_URL,
    engine_options={"echo": False},
    serializer="json",
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
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
)
def ingest(
    s3: S3Details,
    source: SourceDetails,
    upload_action: UploadAction,
    info: IngestionInfo,
) -> dict:
    """Dispatch the fetch_and_parse → index chain for a single document."""
    namespace_id = info.namespace_id
    logger.info(
        "ingest=dispatch action=%s namespace=%s object=%s",
        upload_action,
        namespace_id,
        s3.object,
    )

    group_id = str(info.group_id) if info.group_id is not None else None

    match upload_action:
        case UploadAction.CREATE | UploadAction.UPDATE:
            result = chain(
                fetch_and_parse.s(
                    s3.model_dump(), source.model_dump(), str(namespace_id), group_id
                ),
                index.s(
                    str(namespace_id),
                    group_id,
                    source.model_dump(mode="json"),
                    s3.model_dump(mode="json"),
                ),
            ).apply_async()
            return {"chain_id": str(result.id)}

        case UploadAction.DELETE | UploadAction.AUTO_DELETE:
            result = delete_document.apply_async(
                kwargs={
                    "source": source.model_dump(),
                    "namespace_id": str(namespace_id),
                }
            )
            return {"task_id": str(result.id)}


# ---------------------------------------------------------------------------
# Task 1 — fetch from S3 + call parsing service + save chunks to MinIO
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.fetch_and_parse",
    bind=True,
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    autoretry_for=(pybreaker.CircuitBreakerError,),
    retry_kwargs={"max_retries": 5},
    retry_backoff=True,
    retry_backoff_max=120,
)
def fetch_and_parse(
    self,
    s3: S3Details,
    source: SourceDetails,
    namespace_id: uuid.UUID,
    group_id: str | None = None,
) -> str:
    """Download the document from S3, parse it, persist chunks to MinIO.

    Returns the MinIO object key to be consumed by :func:`index`.

    Failure modes (non-retryable):
        EmptyObjectError   — contract reports size=0; empty file, nothing to index.
        ChunkIntegrityError — parsing service returned chunks with multiple obj_ids.

    Failure modes (retryable, max 3):
        S3IntegrityError   — contract reports size>0 but MinIO returned 0 bytes.
    """
    # Guard 1: empty file — permanent, do not retry.
    if s3.size == 0:
        raise EmptyObjectError(str(source.obj_id))

    task_id = self.request.id or str(uuid.uuid4())
    minio_client = get_s3_client()
    file_bytes = fetch_from_s3(minio_client, s3, logger)

    # Guard 2: MinIO integrity — potentially transient, retry up to 3 times.
    if len(file_bytes) == 0:
        raise self.retry(
            exc=S3IntegrityError(s3.object, s3.size),
            max_retries=3,
            countdown=10,
        )

    chunks = call_parsing_service(
        file_bytes=file_bytes,
        source=source,
        parsing_url=settings.PARSING_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
    )

    # Guard 3: chunk integrity — parsing service contract violation, do not retry.
    obj_ids = {c.obj_id for c in chunks}
    if len(obj_ids) != 1:
        raise ChunkIntegrityError(obj_ids)

    store = ParsedChunkStore(client=minio_client, bucket=settings.PARSED_CHUNKS_BUCKET)
    key = store.save(chunks, task_id)
    logger.info("fetch_and_parse=saved key=%s chunks=%d", key, len(chunks))
    return key


# ---------------------------------------------------------------------------
# Task 2 — load chunks from MinIO + call indexing service + cleanup
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.index",
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 5},
    retry_backoff=True,
    retry_backoff_max=120,
)
def index(
    chunks_key: str,
    namespace_id: uuid.UUID,
    group_id: str | None = None,
    source: SourceDetails | None = None,
    s3: S3Details | None = None,
) -> dict:
    """Load parsed chunks from MinIO, index them, delete the MinIO object.

    *chunks_key* is the MinIO object key returned by :func:`fetch_and_parse`.
    On success the object is removed; on failure it is left for manual
    inspection or dead-letter replay.
    """
    minio_client = get_s3_client()
    store = ParsedChunkStore(client=minio_client, bucket=settings.PARSED_CHUNKS_BUCKET)

    chunks = store.load(chunks_key)
    logger.info("index=loaded key=%s chunks=%d", chunks_key, len(chunks))

    # obj_id uniqueness was validated in fetch_and_parse; extract it here for the result.
    obj_id = str(next(iter({c.obj_id for c in chunks})))

    result = call_indexing_service(
        chunks=chunks,
        namespace_id=namespace_id,
        ingestion_url=settings.INGESTION_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
        group_id=group_id,
    )

    store.delete(chunks_key)
    logger.info("index=cleanup key=%s obj_id=%s", chunks_key, obj_id)

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
    ).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Deletion task — called by ingest for DELETE / AUTO_DELETE actions
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.delete_document",
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 5},
    retry_backoff=True,
    retry_backoff_max=120,
)
def delete_document(source: SourceDetails, namespace_id: uuid.UUID) -> dict:
    """Remove a single document from the indexing service.

    Calls ``DELETE /ingest?namespace=<namespace_id>&obj_id=<obj_id>`` on the
    indexing service, which deletes all vectorstore chunks and record-manager
    entries for the object.
    """
    call_delete_service(
        namespace_id=namespace_id,
        obj_id=str(source.obj_id),
        ingestion_url=settings.INGESTION_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
    )
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
    ).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Namespace deletion task — wipes every document in a namespace
# ---------------------------------------------------------------------------


@app.task(
    name="tasks.delete_namespace",
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 5},
    retry_backoff=True,
    retry_backoff_max=120,
)
def delete_namespace(namespace_id: uuid.UUID) -> dict:
    """Remove all documents for *namespace_id* from the indexing service.

    Calls ``DELETE /ingest?namespace=<namespace_id>`` (no ``source`` param),
    which triggers a full namespace wipe via ``pipeline.aprocess([])``.
    """
    call_delete_service(
        namespace_id=namespace_id,
        ingestion_url=settings.INGESTION_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
    )
    logger.info("delete_namespace=done namespace=%s", namespace_id)
    return {"status": "deleted_namespace", "namespace_id": str(namespace_id)}
