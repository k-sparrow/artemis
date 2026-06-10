import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, BinaryIO, Dict
from uuid import UUID

from fastapi import FastAPI
from minio import Minio
from minio.notificationconfig import NotificationConfig, QueueConfig

from src.backend.storage.api.config import settings
from src.backend.storage.api.dependencies import get_minio_client
from src.lib.backend.logging import configure_logging, get_logger
from src.lib.backend.telemetry import setup_telemetry

__all__ = [
    "create_bucket",
    "link_s3_bucket_with_kafka_event",
    "s3_dump_object",
    "lifespan",
]


def format_kafka_event_arn_sqs(main_name: str) -> str:
    return f"arn:minio:sqs::{main_name}:kafka"


async def create_bucket(minio_client: Minio, bucket_name: str) -> None:
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        minio_client.make_bucket(bucket_name=bucket_name, object_lock=False)


async def link_s3_bucket_with_kafka_event(
    minio_client: Minio, bucket_name: str, event_name: str
) -> None:
    minio_client.set_bucket_notification(
        bucket_name=bucket_name,
        config=NotificationConfig(
            queue_config_list=[
                QueueConfig(
                    queue=format_kafka_event_arn_sqs(main_name=event_name),
                    events=["s3:ObjectCreated:*"],
                )
            ]
        ),
    )


def s3_dump_object(
    minio_client: Minio,
    file_io: BinaryIO,
    content_type: str,
    size: int,
    file_id: UUID,
    task_id: UUID,
    s3_metadata: Dict[str, str] | None = None,
) -> None:
    file_io.seek(0)
    minio_client.put_object(
        bucket_name=settings.S3_ARTEMIS_BUCKET,
        object_name=f"artemis/private/{str(file_id)}",
        data=file_io,
        length=size,
        content_type=content_type,
        metadata={
            "file_id": str(file_id),
            "task_id": str(task_id),
            **(s3_metadata or {}),
        },
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_telemetry("backend-storage")
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("storage")
    logger.info("lifespan_started")

    minio_client = await get_minio_client()
    await create_bucket(
        minio_client=minio_client, bucket_name=settings.S3_ARTEMIS_BUCKET
    )
    await link_s3_bucket_with_kafka_event(
        minio_client=minio_client,
        bucket_name=settings.S3_ARTEMIS_BUCKET,
        event_name=settings.S3_ARTEMIS_BUCKET_KAFKA_EVENT,
    )

    # Create Postgres tables (idempotent; Alembic handles this in production).
    from sqlalchemy.ext.asyncio import create_async_engine
    from src.backend.storage.api.models import Base  # noqa: F401 — registers models

    engine = create_async_engine(settings.SQL_DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    logger.info("lifespan_ready", bucket=settings.S3_ARTEMIS_BUCKET)
    yield
    logger.info("lifespan_ended")
