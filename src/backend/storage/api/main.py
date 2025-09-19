from uuid import UUID, uuid4
from typing import List

import logging
import logging.config

from fastapi import FastAPI, File, UploadFile, Request, Form

from src.backend.storage.api.config import storage_settings as settings, LOGGING_CONFIG
from src.backend.storage.api.dependencies import (
    minio_client_dependency,
)
from src.backend.storage.api.utils import (
    lifespan,
)

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


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
    files: List[UploadFile] = File(...),
    user_id: UUID = Form(),
    chat_id: UUID = Form(),
):
    logger.info(
        f"Received {len(files)} files to upload from user[{user_id}], chat[{chat_id}]"
    )
    file_ids = [uuid4() for _ in range(0, len(files))]
    task_ids = [uuid4() for _ in range(0, len(files))]
    for file_, file_id, task_id in zip(files, file_ids, task_ids):
        logger.info(f"Received upload file: {file_.filename}...")
        await file_.seek(0)
        minio_client.put_object(
            bucket_name=settings.S3_VENUS_BUCKET,
            object_name=f"venus/private/{str(file_id)}",
            data=file_.file,
            length=file_.size,
            content_type=file_.content_type,
            metadata={
                "user_id": str(user_id),
                "chat_id": str(chat_id),
                "file_id": str(chat_id),
                "task_id": str(task_id),
                "filename": file_.filename,
            },
        )
    return {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "file_ids": [str(file_id) for file_id in file_ids],
        "task_ids": [str(task_id) for task_id in task_ids],
    }
