"""Celery tasks for the Artemis ingestion pipeline.

Chain structure
---------------
The Kafka HTTP Sink calls ``tasks.ingest``, which resolves the namespace UUID
and dispatches the two-task chain:

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
from celery.utils.log import get_task_logger

from src.backend.controller.lib.schemas import (
    IngestionInfo,
    S3Details,
    SourceDetails,
    UploadAction,
)
from src.backend.controller.worker.backend.database import DatabaseBackend
from src.backend.controller.worker.celery import app
from src.backend.controller.worker.config import settings
from src.backend.controller.worker.dependencies import get_s3_client
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

    match upload_action:
        case UploadAction.CREATE | UploadAction.UPDATE:
            result = chain(
                fetch_and_parse.s(
                    s3.model_dump(), source.model_dump(), str(namespace_id)
                ),
                index.s(str(namespace_id)),
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
    pydantic=True,
    backend=_db_backend,
    result_serializer="json",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 5},
    retry_backoff=True,
    retry_backoff_max=120,
)
def fetch_and_parse(
    s3: S3Details,
    source: SourceDetails,
    namespace_id: uuid.UUID,
) -> str:
    """Download the document from S3, parse it, persist chunks to MinIO.

    Returns the MinIO object key to be consumed by :func:`index`.
    """
    task_id = fetch_and_parse.request.id or str(uuid.uuid4())

    minio_client = get_s3_client()

    file_bytes = fetch_from_s3(minio_client, s3, logger)

    chunks = call_parsing_service(
        file_bytes=file_bytes,
        source=source,
        parsing_url=settings.PARSING_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
    )

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
def index(chunks_key: str, namespace_id: uuid.UUID) -> dict:
    """Load parsed chunks from MinIO, index them, delete the MinIO object.

    *chunks_key* is the MinIO object key returned by :func:`fetch_and_parse`.
    On success the object is removed; on failure it is left for manual
    inspection or dead-letter replay.
    """
    minio_client = get_s3_client()
    store = ParsedChunkStore(client=minio_client, bucket=settings.PARSED_CHUNKS_BUCKET)

    chunks = store.load(chunks_key)
    logger.info("index=loaded key=%s chunks=%d", chunks_key, len(chunks))

    result = call_indexing_service(
        chunks=chunks,
        namespace_id=namespace_id,
        ingestion_url=settings.INGESTION_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
    )

    store.delete(chunks_key)
    logger.info("index=cleanup key=%s", chunks_key)

    return result


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

    Calls ``DELETE /ingest?namespace=<namespace_id>&source=<path>`` on the
    indexing service, which deletes all vectorstore chunks and record-manager
    entries for the source.
    """
    if source.path is None:
        logger.warning(
            "delete_document=skipped namespace=%s source.path=None",
            namespace_id,
        )
        return {"status": "skipped", "reason": "source_path_missing"}

    call_delete_service(
        namespace_id=namespace_id,
        source=source.path,
        ingestion_url=settings.INGESTION_SERVICE_URL,
        timeout=settings.HTTPX_TIMEOUT,
        logger=logger,
    )
    logger.info(
        "delete_document=done namespace=%s source=%s", namespace_id, source.path
    )
    return {
        "status": "deleted",
        "source": source.path,
        "namespace_id": str(namespace_id),
    }


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
