import logging
from typing import Dict

from celery.utils.log import get_task_logger

from src.backend.controller.lib.schemas import (
    S3Details,
    SourceDetails,
    PrivateIngestionInfo,
    LibraryIngestionInfo,
    UploadAction,
    UpsertionMetaInfoType,
)
from src.backend.controller.worker.celery import app
from src.backend.controller.worker.config import settings
from src.backend.controller.worker.utils import s3_to_ingestion
from src.backend.controller.worker.backend.database import DatabaseBackend

logger = get_task_logger(__name__)
logger.setLevel(logging.DEBUG)


@app.task(
    name="tasks.ingestion.private",
    pydantic=True,
    backend=DatabaseBackend(
        app=app,
        dburi=settings.BACKEND_RESULT_URL,
        engine_options={"echo": True},
        serializer="json",
    ),
    result_serializer="json",
)
def ingest_private(
    s3: S3Details,
    source: SourceDetails,
    upload_action: UploadAction,
    info: PrivateIngestionInfo,
) -> Dict[str, str]:
    return s3_to_ingestion(
        s3=s3,
        source=source,
        upload_action=upload_action,
        info=info,
        ingestion_url=settings.PRIVATE_INGESTION_SERVICE_URL,
        upsertion_type=UpsertionMetaInfoType.PRIVATE,
        logger=logger,
        logger_prefix="[PRIVATE]",
    )


@app.task(
    name="tasks.ingestion.library",
    pydantic=True,
    backend=DatabaseBackend(
        app=app,
        dburi=settings.BACKEND_RESULT_URL,
        engine_options={"echo": True},
        serializer="json",
    ),
    result_serializer="json",
)
def ingest_library(
    s3: S3Details,
    source: SourceDetails,
    upload_action: UploadAction,
    info: LibraryIngestionInfo,
) -> Dict[str, str]:
    return s3_to_ingestion(
        s3=s3,
        source=source,
        upload_action=upload_action,
        info=info,
        ingestion_url=settings.LIBRARY_INGESTION_SERVICE_URL,
        upsertion_type=UpsertionMetaInfoType.LIBRARY,
        logger=logger,
        logger_prefix="[LIBRARY]",
    )
