"""File ingestion service functions."""

from __future__ import annotations

import io
import uuid
import sqlalchemy as sa
from opentelemetry import trace as otel_trace
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
from src.backend.storage.api.service import _fetch_namespace, s3_key
from src.backend.storage.api.tombstone import tombstone_objects
from src.lib.core.ingestion.contract import (
    IngestionInfo,
    IngestionTaskDetails,
    S3Details,
    SourceDetails,
)


def _stamp_task_span(
    task_id: uuid.UUID,
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID,
) -> None:
    span = otel_trace.get_current_span()
    if span.is_recording():
        span.set_attribute("artemis.task_id", str(task_id))
        span.set_attribute("artemis.namespace_id", str(namespace_id))
        span.set_attribute("artemis.obj_id", str(obj_id))


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
    caller_owner_id: uuid.UUID,
    filename: str | None,
    content_type: str | None,
    data: bytes,
    group_id: uuid.UUID | None = None,
) -> uuid.UUID:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        require_write=True,
    )
    task_id = uuid.uuid4()
    source_label = filename or str(uuid.uuid4())
    obj_id = uuid.uuid5(namespace_id, source_label)
    _stamp_task_span(task_id=task_id, namespace_id=namespace_id, obj_id=obj_id)
    key = s3_key(namespace_id, obj_id, filename)
    resolved_content_type = content_type or "application/octet-stream"

    details = IngestionTaskDetails(
        upload_action=IngestionTaskType.CREATE,
        s3=S3Details(bucket=bucket, object=key, size=len(data)),
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
        object_name=key,
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
    caller_owner_id: uuid.UUID,
    obj_id: uuid.UUID,
    filename: str | None,
    content_type: str | None,
    data: bytes,
    group_id: uuid.UUID | None = None,
) -> uuid.UUID:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        require_write=True,
    )
    existing = await _fetch_ingested_object(
        session=session, namespace_id=namespace_id, obj_id=obj_id
    )
    task_id = uuid.uuid4()
    _stamp_task_span(task_id=task_id, namespace_id=namespace_id, obj_id=obj_id)
    # Use the provided filename as the source label; fall back to str(obj_id)
    # if not supplied so source.source is always non-null.
    source_label = filename or str(obj_id)
    # Fall back to the existing object's filename (not source_label) for the
    # key's extension so an un-supplied filename still resolves to the same
    # key the original upload wrote to, rather than orphaning it.
    key = s3_key(namespace_id, obj_id, filename or existing.source)
    resolved_content_type = content_type or "application/octet-stream"

    details = IngestionTaskDetails(
        upload_action=IngestionTaskType.MODIFY,
        s3=S3Details(bucket=bucket, object=key, size=len(data)),
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
        object_name=key,
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
    caller_owner_id: uuid.UUID,
    obj_id: uuid.UUID,
    task_id: uuid.UUID,
) -> None:
    """Tombstone an object by uploading a 0-byte marker object.

    The MinIO PUT fires an S3 event → Kafka → RabbitMQ sink → Celery worker,
    which performs the actual hard-delete of the Qdrant vectors and S3 objects.
    """
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        require_write=True,
    )
    ingested = await _fetch_ingested_object(
        session=session, namespace_id=namespace_id, obj_id=obj_id
    )
    _stamp_task_span(task_id=task_id, namespace_id=namespace_id, obj_id=obj_id)
    key = s3_key(namespace_id, obj_id, ingested.source)

    details = IngestionTaskDetails(
        upload_action=IngestionTaskType.DELETE,
        s3=S3Details(bucket=bucket, object=key, size=0),
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
        object_name=key,
        data=io.BytesIO(b""),
        length=0,
        metadata={
            "task_id": str(task_id),
            "contract": details.model_dump_json(),
        },
    )


async def delete_group(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    group_id: uuid.UUID,
) -> list[uuid.UUID]:
    """Tombstone all objects belonging to a group."""
    return await tombstone_objects(
        minio=minio,
        session=session,
        bucket=bucket,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        group_id=group_id,
    )


async def get_object(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    obj_id: uuid.UUID,
) -> IngestedObject:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
    )
    return await _fetch_ingested_object(
        session=session, namespace_id=namespace_id, obj_id=obj_id
    )


async def list_groups(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
) -> list[sa.Row]:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
    )
    result = await session.execute(
        sa.select(IngestedObject.group_id, sa.func.count().label("object_count"))
        .where(
            IngestedObject.namespace_id == namespace_id,
            IngestedObject.group_id.is_not(None),
        )
        .group_by(IngestedObject.group_id)
    )
    return list(result.all())


async def list_files(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    group_id: uuid.UUID | None = None,
    limit: int | None = None,
    order: str = "asc",
) -> list[IngestedObject]:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
    )
    filters = [IngestedObject.namespace_id == namespace_id]
    if group_id is not None:
        filters.append(IngestedObject.group_id == group_id)
    order_col = (
        IngestedObject.ingested_at.desc()
        if order == "desc"
        else IngestedObject.ingested_at.asc()
    )
    q = sa.select(IngestedObject).where(*filters).order_by(order_col)
    if limit is not None:
        q = q.limit(limit)
    result = await session.execute(q)
    return list(result.scalars().all())


async def list_tasks(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
) -> list[IngestionTask]:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
    )
    result = await session.execute(
        sa.select(IngestionTask).where(IngestionTask.namespace_id == namespace_id)
    )
    return list(result.scalars().all())


async def get_task_status(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    task_id: uuid.UUID,
) -> IngestionTask:
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
    )
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
