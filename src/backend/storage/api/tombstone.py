"""Shared tombstone helpers for the storage service.

A tombstone is a 0-byte MinIO object with upload_action=DELETE baked into the
S3 metadata. MinIO fires an S3 event which flows through Kafka → RabbitMQ →
Celery, where the worker performs the actual hard-delete of Qdrant vectors and
S3 files.
"""

from __future__ import annotations

import io
import uuid

import sqlalchemy as sa
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.storage.api.models import IngestedObject, IngestionTaskType
from src.backend.storage.api.service import _fetch_namespace
from src.lib.core.ingestion.contract import (
    IngestionInfo,
    IngestionTaskDetails,
    S3Details,
    SourceDetails,
)


def _s3_key(namespace_id: uuid.UUID, obj_id: uuid.UUID) -> str:
    return f"{namespace_id}/{obj_id}"


async def tombstone_objects(
    minio: Minio,
    session: AsyncSession,
    bucket: str,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    group_id: uuid.UUID | None = None,
) -> list[uuid.UUID]:
    """Tombstone objects in a namespace, optionally scoped to a group_id.

    Writes a 0-byte MinIO marker per object with upload_action=DELETE so the
    async pipeline (Kafka → Celery) cleans up Qdrant vectors and S3 files.
    Returns task_ids dispatched, one per object.
    """
    await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        require_write=True,
    )
    filters = [IngestedObject.namespace_id == namespace_id]
    if group_id is not None:
        filters.append(IngestedObject.group_id == group_id)
    result = await session.execute(sa.select(IngestedObject).where(*filters))
    objects = list(result.scalars().all())

    task_ids = []
    for obj in objects:
        task_id = uuid.uuid4()
        s3_key = _s3_key(namespace_id, obj.id)
        details = IngestionTaskDetails(
            upload_action=IngestionTaskType.DELETE,
            s3=S3Details(bucket=bucket, object=s3_key, size=0),
            source=SourceDetails(
                source=obj.source,
                content_type=obj.content_type,
                obj_id=obj.id,
                object_type=obj.object_type,
            ),
            info=IngestionInfo(
                namespace_id=namespace_id,
                group_id=group_id if group_id is not None else obj.group_id,
            ),
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
        task_ids.append(task_id)
    return task_ids
