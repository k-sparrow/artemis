"""FastAPI router for object ingestion dispatch and observability.

Routes are mounted under /namespaces in main.py so all paths here are
relative to that prefix: /{namespace_id}/objects, /{namespace_id}/tasks, etc.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Query, UploadFile, status

from src.backend.storage.api.config import settings
from src.backend.storage.api.dependencies import (
    db_session_dependency,
    minio_client_dependency,
)
from src.backend.storage.api.files import service
from src.backend.storage.api.files.schemas import (
    GroupDeleteResponse,
    IngestedObjectResponse,
    IngestionTaskResponse,
    ObjectUploadResponse,
)
from src.lib.backend.logging import get_logger

router = APIRouter(tags=["objects"])
log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Ingestion dispatch
# ---------------------------------------------------------------------------


@router.post(
    "/{namespace_id}/objects",
    response_model=ObjectUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_object_endpoint(
    namespace_id: uuid.UUID,
    file: UploadFile,
    session: db_session_dependency,
    minio: minio_client_dependency,
    group_id: uuid.UUID | None = Query(default=None),
) -> ObjectUploadResponse:
    data = await file.read()
    task_id = await service.upload_file(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        filename=file.filename,
        content_type=file.content_type,
        data=data,
        group_id=group_id,
    )
    log.info(
        "object_uploaded",
        namespace=namespace_id,
        filename=file.filename,
        task_id=task_id,
    )
    return ObjectUploadResponse(task_id=task_id)


@router.put(
    "/{namespace_id}/objects/{obj_id}",
    response_model=ObjectUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def reingest_object_endpoint(
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID,
    file: UploadFile,
    session: db_session_dependency,
    minio: minio_client_dependency,
    group_id: uuid.UUID | None = Query(default=None),
) -> ObjectUploadResponse:
    data = await file.read()
    task_id = await service.reingest_file(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        obj_id=obj_id,
        filename=file.filename,
        content_type=file.content_type,
        data=data,
        group_id=group_id,
    )
    log.info(
        "object_reingested", namespace=namespace_id, obj_id=obj_id, task_id=task_id
    )
    return ObjectUploadResponse(task_id=task_id)


@router.delete(
    "/{namespace_id}/objects",
    response_model=GroupDeleteResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def delete_group_endpoint(
    namespace_id: uuid.UUID,
    group_id: uuid.UUID,
    session: db_session_dependency,
    minio: minio_client_dependency,
) -> GroupDeleteResponse:
    task_ids = await service.delete_group(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        group_id=group_id,
    )
    log.info(
        "group_tombstoned",
        namespace=namespace_id,
        group_id=group_id,
        count=len(task_ids),
    )
    return GroupDeleteResponse(task_ids=task_ids)


@router.delete("/{namespace_id}/objects/{obj_id}", status_code=status.HTTP_202_ACCEPTED)
async def delete_object_endpoint(
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID,
    session: db_session_dependency,
    minio: minio_client_dependency,
) -> None:
    task_id = uuid.uuid4()
    await service.delete_file(
        minio=minio,
        session=session,
        bucket=settings.S3_ARTEMIS_BUCKET,
        namespace_id=namespace_id,
        obj_id=obj_id,
        task_id=task_id,
    )
    log.info(
        "object_tombstoned", namespace=namespace_id, obj_id=obj_id, task_id=task_id
    )


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


@router.get("/{namespace_id}/objects", response_model=list[IngestedObjectResponse])
async def list_objects_endpoint(
    namespace_id: uuid.UUID,
    session: db_session_dependency,
    group_id: uuid.UUID | None = Query(default=None),
) -> list[IngestedObjectResponse]:
    objects = await service.list_files(
        session=session, namespace_id=namespace_id, group_id=group_id
    )
    return [IngestedObjectResponse.model_validate(o) for o in objects]


@router.get("/{namespace_id}/tasks", response_model=list[IngestionTaskResponse])
async def list_tasks_endpoint(
    namespace_id: uuid.UUID,
    session: db_session_dependency,
) -> list[IngestionTaskResponse]:
    tasks = await service.list_tasks(session=session, namespace_id=namespace_id)
    return [IngestionTaskResponse.model_validate(t) for t in tasks]


@router.get("/{namespace_id}/tasks/{task_id}", response_model=IngestionTaskResponse)
async def get_task_status_endpoint(
    namespace_id: uuid.UUID,
    task_id: uuid.UUID,
    session: db_session_dependency,
) -> IngestionTaskResponse:
    task = await service.get_task_status(
        session=session, namespace_id=namespace_id, task_id=task_id
    )
    return IngestionTaskResponse.model_validate(task)
