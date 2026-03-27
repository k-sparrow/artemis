"""File ingestion service functions."""

from __future__ import annotations

import io
import uuid
from typing import Any

import sqlalchemy as sa
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.storage.api.files.exceptions import (
    IngestedFileNotFoundError,
    TaskNotFoundError,
)
from src.backend.storage.api.models import IngestedFile, IngestionTaskType
from src.backend.storage.api.service import _fetch_namespace


def _s3_key(namespace_id: uuid.UUID, file_id: uuid.UUID) -> str:
    return f"{namespace_id}/{file_id}"


async def _fetch_ingested_file(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    file_id: uuid.UUID,
) -> IngestedFile:
    result = await session.execute(
        sa.select(IngestedFile).where(
            IngestedFile.id == file_id,
            IngestedFile.namespace_id == namespace_id,
        )
    )
    file = result.scalar_one_or_none()
    if file is None:
        raise IngestedFileNotFoundError()
    return file


async def upload_file(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    filename: str | None,
    content_type: str | None,
    data: bytes,
) -> tuple[uuid.UUID, str]:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    task_id = uuid.uuid4()
    file_id = uuid.uuid4()
    s3_key = _s3_key(namespace_id, file_id)
    minio.put_object(
        bucket_name=bucket,
        object_name=s3_key,
        data=io.BytesIO(data),
        length=len(data),
        content_type=content_type or "application/octet-stream",
        metadata={
            "task_id": str(task_id),
            "namespace_id": str(namespace_id),
            "task_type": IngestionTaskType.CREATE,
        },
    )
    return task_id, s3_key


async def reingest_file(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    file_id: uuid.UUID,
    content_type: str | None,
    data: bytes,
) -> tuple[uuid.UUID, str]:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    await _fetch_ingested_file(
        session=session, namespace_id=namespace_id, file_id=file_id
    )
    task_id = uuid.uuid4()
    s3_key = _s3_key(namespace_id, file_id)
    minio.put_object(
        bucket_name=bucket,
        object_name=s3_key,
        data=io.BytesIO(data),
        length=len(data),
        content_type=content_type or "application/octet-stream",
        metadata={
            "task_id": str(task_id),
            "namespace_id": str(namespace_id),
            "task_type": IngestionTaskType.MODIFY,
        },
    )
    return task_id, s3_key


async def delete_file(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    file_id: uuid.UUID,
    task_id: uuid.UUID,
) -> None:
    """Tombstone a file by uploading a 0-byte marker object.

    Implementation note (epic 1.2)
    --------------------------------
    This 0-byte tombstone is a **temporary** workaround for the Kafka middleman.
    The MinIO PUT fires an S3 event → Kafka → RabbitMQ sink → Celery worker, which
    then performs the actual hard-delete of the Qdrant vectors and S3 objects.

    Once epic 1.2 lands and the storage service can call ``apply_async`` directly,
    dismantle this as follows:
    1. Remove the ``minio.put_object`` call here.
    2. Call ``delete_task.apply_async(kwargs={...}, task_id=str(task_id))``.
    3. Drop the tombstone-detection branch in the worker.
    """
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    await _fetch_ingested_file(
        session=session, namespace_id=namespace_id, file_id=file_id
    )
    s3_key = _s3_key(namespace_id, file_id)
    minio.put_object(
        bucket_name=bucket,
        object_name=s3_key,
        data=io.BytesIO(b""),
        length=0,
        metadata={
            "task_id": str(task_id),
            "namespace_id": str(namespace_id),
            "task_type": IngestionTaskType.DELETE,
        },
    )


async def list_files(
    session: AsyncSession,
    namespace_id: uuid.UUID,
) -> list[IngestedFile]:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    result = await session.execute(
        sa.select(IngestedFile).where(IngestedFile.namespace_id == namespace_id)
    )
    return list(result.scalars().all())


async def get_task_status(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    task_id: uuid.UUID,
) -> dict[str, Any]:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    result = await session.execute(
        sa.text(
            "SELECT task_id, status, result, traceback, date_done "
            "FROM apollo_celery_taskmeta "
            "WHERE task_id = :task_id"
        ),
        {"task_id": str(task_id)},
    )
    row = result.mappings().one_or_none()
    if row is None:
        raise TaskNotFoundError()
    return dict(row)
