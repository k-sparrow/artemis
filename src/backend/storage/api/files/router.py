"""FastAPI router for file ingestion dispatch and observability.

Routes are mounted under /namespaces in main.py so all paths here are
relative to that prefix: /{namespace_id}/files, /{namespace_id}/tasks, etc.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, UploadFile, status

from src.backend.storage.api.config import settings
from src.backend.storage.api.dependencies import (
    db_session_dependency,
    minio_client_dependency,
)
from src.backend.storage.api.files import service
from src.backend.storage.api.files.schemas import (
    FileUploadResponse,
    IngestedFileResponse,
    TaskStatusResponse,
)
from src.lib.backend.logging import get_logger

router = APIRouter(tags=["files"])
log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Ingestion dispatch
# ---------------------------------------------------------------------------


@router.post(
    "/{namespace_id}/files",
    response_model=FileUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_file_endpoint(
    namespace_id: uuid.UUID,
    file: UploadFile,
    session: db_session_dependency,
    minio: minio_client_dependency,
) -> FileUploadResponse:
    data = await file.read()
    task_id, s3_key = await service.upload_file(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        filename=file.filename,
        content_type=file.content_type,
        data=data,
    )
    log.info(
        "file_uploaded", namespace=namespace_id, filename=file.filename, task_id=task_id
    )
    # TODO(epic-1.2): apply_async(ingest_task, task_id=task_id, ...)
    return FileUploadResponse(task_id=task_id, s3_key=s3_key)


@router.put(
    "/{namespace_id}/files/{file_id}",
    response_model=FileUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def reingest_file_endpoint(
    namespace_id: uuid.UUID,
    file_id: uuid.UUID,
    file: UploadFile,
    session: db_session_dependency,
    minio: minio_client_dependency,
) -> FileUploadResponse:
    data = await file.read()
    task_id, s3_key = await service.reingest_file(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        file_id=file_id,
        content_type=file.content_type,
        data=data,
    )
    log.info(
        "file_reingested", namespace=namespace_id, file_id=file_id, task_id=task_id
    )
    # TODO(epic-1.2): apply_async(ingest_task, task_id=task_id, ...)
    return FileUploadResponse(task_id=task_id, s3_key=s3_key)


@router.delete("/{namespace_id}/files/{file_id}", status_code=status.HTTP_202_ACCEPTED)
async def delete_file_endpoint(
    namespace_id: uuid.UUID,
    file_id: uuid.UUID,
    session: db_session_dependency,
    minio: minio_client_dependency,
) -> None:
    task_id = uuid.uuid4()
    await service.delete_file(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        file_id=file_id,
        task_id=task_id,
    )
    log.info(
        "file_tombstoned", namespace=namespace_id, file_id=file_id, task_id=task_id
    )
    # TODO(epic-1.2): apply_async(delete_task, namespace_id=..., file_id=...)


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


@router.get("/{namespace_id}/files", response_model=list[IngestedFileResponse])
async def list_files_endpoint(
    namespace_id: uuid.UUID,
    session: db_session_dependency,
) -> list[IngestedFileResponse]:
    files = await service.list_files(session=session, namespace_id=namespace_id)
    return [IngestedFileResponse.model_validate(f) for f in files]


@router.get("/{namespace_id}/tasks", response_model=list[IngestedFileResponse])
async def list_tasks_endpoint(
    namespace_id: uuid.UUID,
    session: db_session_dependency,
) -> list[IngestedFileResponse]:
    """Return terminal-state task records for *namespace_id* from ingested_file."""
    files = await service.list_files(session=session, namespace_id=namespace_id)
    return [IngestedFileResponse.model_validate(f) for f in files]


@router.get("/{namespace_id}/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task_status_endpoint(
    namespace_id: uuid.UUID,
    task_id: uuid.UUID,
    session: db_session_dependency,
) -> TaskStatusResponse:
    row = await service.get_task_status(
        session=session, namespace_id=namespace_id, task_id=task_id
    )
    return TaskStatusResponse(**row)
