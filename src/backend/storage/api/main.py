from contextlib import asynccontextmanager
from typing import AsyncIterator
import logging
import logging.config

from fastapi import FastAPI, File, UploadFile, Request

from src.backend.storage.api.config import storage_settings as settings, LOGGING_CONFIG
from src.backend.storage.api.dependencies import (
    get_minio_client,
    minio_client_dependency,
)
from src.backend.storage.api.utils import create_bucket, link_s3_bucket_with_kafka_event

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    minio_client = await get_minio_client()

    # create the bucket if it does not exist
    # and link between the bucket and the event topic
    await create_bucket(minio_client=minio_client, bucket_name=settings.S3_VENUS_BUCKET)
    await link_s3_bucket_with_kafka_event(
        minio_client=minio_client,
        bucket_name=settings.S3_VENUS_BUCKET,
        event_name_name=settings.S3_VENUS_BUCKET_KAFKA_EVENT,
    )
    yield


app = FastAPI(
    title="Venus Storage Service",
    description="S3 object storage for Venus project",
    lifespan=lifespan,
)


# here arrive update notifications from Kafka
# about fresh uploads to the
@app.post("/library")
async def ee(request: Request):
    logger.info(f"{request.method} {request.url}")
    logger.info(f"Request contents: {await request.body()}")
    return {"message": "Hello from Venus!"}


@app.post("/upload")
async def upload(
    minio_client: minio_client_dependency,
    file: UploadFile = File(...),
):
    logger.info(f"Received upload file: {file.filename}...")
    minio_client.put_object(
        bucket_name=settings.S3_VENUS_BUCKET,
        object_name=f"venus/private/{file.filename}",
        data=file.file,
        content_type=file.content_type,
    )

    return {"message": "File uploaded successfully"}
