import logging
import logging.config
from uuid import UUID, uuid4
from typing import List

from fastapi import UploadFile
from minio import Minio

from src.backend.storage.api.config import LOGGING_CONFIG
from src.backend.storage.api.utils import (
    s3_dump_object,
)

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def upload(minio_client: Minio, files: List[UploadFile], user_id: UUID, chat_id: UUID):
    logger.info(
        f"Received {len(files)} files to upload from user[{user_id}], chat[{chat_id}]"
    )
    file_ids = [uuid4() for _ in range(0, len(files))]
    task_ids = [uuid4() for _ in range(0, len(files))]
    for file_, file_id, task_id in zip(files, file_ids, task_ids):
        logger.info(f"Received upload file: {file_.filename}...")
        s3_dump_object(
            minio_client,
            file_io=file_.file,
            content_type=file_.content_type,
            size=file_.size,
            task_id=task_id,
            file_id=file_id,
            s3_metadata={
                "user_id": str(user_id),
                "chat_id": str(chat_id),
                "filename": file_.filename,
            },
        )
    return {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "file_ids": [str(file_id) for file_id in file_ids],
        "task_ids": [str(task_id) for task_id in task_ids],
    }
