import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import get_s3_client
from src.lib.backend.logging import configure_logging, get_logger
from src.lib.core.adapters.stores.minio.blob import MinioBlobStore

__all__ = [
    "lifespan",
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("parsing")

    # Create the output buckets once at startup (writer-store ensure-bucket is
    # explicit; construction is pure). S3 settings are required, so this always
    # runs — a misconfigured/unreachable MinIO fails fast here.
    s3 = get_s3_client()
    for bucket in (
        settings.PARSED_ARTIFACTS_BUCKET,
        settings.REPLAY_CACHE_BUCKET,
    ):
        MinioBlobStore(s3, bucket).ensure_bucket()
        logger.info("bucket_ready", bucket=bucket)

    logger.info("lifespan_started")
    yield
    logger.info("lifespan_ended")
