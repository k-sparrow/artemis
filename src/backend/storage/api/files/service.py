"""File ingestion service functions."""

from __future__ import annotations

import io
import uuid
import sqlalchemy as sa
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.storage.api.files.exceptions import (
    IngestedFileNotFoundError,
    TaskNotFoundError,
)
from src.backend.storage.api.models import (
    IngestedObject,
    IngestionTask,
    IngestionTaskType,
)
from src.backend.storage.api.service import _fetch_namespace
from src.lib.core.ingestion.contract import (
    IngestionInfo,
    IngestionTaskDetails,
    S3Details,
    SourceDetails,
)


def _s3_key(namespace_id: uuid.UUID, obj_id: uuid.UUID) -> str:
    return f"{namespace_id}/{obj_id}"


async def _fetch_ingested_object(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID,
) -> IngestedObject:
    result = await session.execute(
        sa.select(IngestedObject).where(
            IngestedObject.id == obj_id,
            IngestedObject.namespace_id == namespace_id,
        )
    )
    obj = result.scalar_one_or_none()
    if obj is None:
        raise IngestedFileNotFoundError()
    return obj


async def upload_file(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    filename: str | None,
    content_type: str | None,
    data: bytes,
    group_id: uuid.UUID | None = None,
) -> uuid.UUID:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    task_id = uuid.uuid4()
    source_label = filename or str(uuid.uuid4())
    obj_id = uuid.uuid5(namespace_id, source_label)
    s3_key = _s3_key(namespace_id, obj_id)
    resolved_content_type = content_type or "application/octet-stream"

    details = IngestionTaskDetails(
        upload_action=IngestionTaskType.CREATE,
        s3=S3Details(bucket=bucket, object=s3_key, size=len(data)),
        source=SourceDetails(
            source=source_label,
            content_type=resolved_content_type,
            obj_id=obj_id,
            object_type="file",
        ),
        info=IngestionInfo(namespace_id=namespace_id, group_id=group_id),
    )

    minio.put_object(
        bucket_name=bucket,
        object_name=s3_key,
        data=io.BytesIO(data),
        length=len(data),
        content_type=resolved_content_type,
        metadata={
            "task_id": str(task_id),
            "contract": details.model_dump_json(),
        },
    )
    return task_id


async def reingest_file(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID,
    filename: str | None,
    content_type: str | None,
    data: bytes,
    group_id: uuid.UUID | None = None,
) -> uuid.UUID:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    await _fetch_ingested_object(
        session=session, namespace_id=namespace_id, obj_id=obj_id
    )
    task_id = uuid.uuid4()
    # Use the provided filename as the source label; fall back to str(obj_id)
    # if not supplied so source.source is always non-null.
    source_label = filename or str(obj_id)
    s3_key = _s3_key(namespace_id, obj_id)
    resolved_content_type = content_type or "application/octet-stream"

    details = IngestionTaskDetails(
        upload_action=IngestionTaskType.MODIFY,
        s3=S3Details(bucket=bucket, object=s3_key, size=len(data)),
        source=SourceDetails(
            source=source_label,
            content_type=resolved_content_type,
            obj_id=obj_id,
            object_type="file",
        ),
        info=IngestionInfo(namespace_id=namespace_id, group_id=group_id),
    )

    minio.put_object(
        bucket_name=bucket,
        object_name=s3_key,
        data=io.BytesIO(data),
        length=len(data),
        content_type=resolved_content_type,
        metadata={
            "task_id": str(task_id),
            "contract": details.model_dump_json(),
        },
    )
    return task_id


async def delete_file(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID,
    task_id: uuid.UUID,
) -> None:
    """Tombstone an object by uploading a 0-byte marker object.

    The MinIO PUT fires an S3 event → Kafka → RabbitMQ sink → Celery worker,
    which performs the actual hard-delete of the Qdrant vectors and S3 objects.
    """
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    ingested = await _fetch_ingested_object(
        session=session, namespace_id=namespace_id, obj_id=obj_id
    )
    s3_key = _s3_key(namespace_id, obj_id)

    details = IngestionTaskDetails(
        upload_action=IngestionTaskType.DELETE,
        s3=S3Details(bucket=bucket, object=s3_key, size=0),
        source=SourceDetails(
            source=ingested.source,
            content_type=ingested.content_type,
            obj_id=obj_id,
            object_type=ingested.object_type,
        ),
        info=IngestionInfo(namespace_id=namespace_id, group_id=ingested.group_id),
    )

    minio.put_object(
        bucket_name=bucket,
        object_name=s3_key,
        data=io.BytesIO(b""),
        length=0,
        metadata={
            "task_id": str(task_id),
            "contract": details.model_dump_json(),
        },
    )


async def list_files(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    group_id: uuid.UUID | None = None,
) -> list[IngestedObject]:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    filters = [IngestedObject.namespace_id == namespace_id]
    if group_id is not None:
        filters.append(IngestedObject.group_id == group_id)
    result = await session.execute(sa.select(IngestedObject).where(*filters))
    return list(result.scalars().all())


async def list_tasks(
    session: AsyncSession,
    namespace_id: uuid.UUID,
) -> list[IngestionTask]:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    result = await session.execute(
        sa.select(IngestionTask).where(IngestionTask.namespace_id == namespace_id)
    )
    return list(result.scalars().all())


async def get_task_status(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    task_id: uuid.UUID,
) -> IngestionTask:
    await _fetch_namespace(session=session, namespace_id=namespace_id)
    result = await session.execute(
        sa.select(IngestionTask).where(
            IngestionTask.task_id == task_id,
            IngestionTask.namespace_id == namespace_id,
        )
    )
    task = result.scalar_one_or_none()
    if task is None:
        raise TaskNotFoundError()
    return task
