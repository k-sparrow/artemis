from io import BytesIO
import json
from typing import List
from uuid import UUID, uuid4

from fastapi import UploadFile
from minio import Minio

from src.backend.storage.api.private.exceptions import FilesAndFileIdsLengthMismatch
from src.backend.storage.api.private.schemas import ObjectOperation
from src.backend.storage.api.private.utils import (
    s3_dump_file,
)

__all__ = [
    "s3_batch_upload",
]


def s3_batch_upload(
    minio_client: Minio,
    user_id: UUID,
    chat_id: UUID,
    files: List[UploadFile],
    file_ids: List[UUID] = [],
):
    operation: ObjectOperation = ObjectOperation.MODIFY
    if not file_ids:
        file_ids = [uuid4() for _ in range(0, len(files))]
        operation = ObjectOperation.CREATE

    elif len(file_ids) != len(files):
        raise FilesAndFileIdsLengthMismatch(
            n_files=len(files), n_file_ids=len(file_ids)
        )

    task_ids = [uuid4() for _ in range(0, len(files))]
    for file_, file_id, task_id in zip(files, file_ids, task_ids):
        s3_dump_file(
            minio_client,
            file_io=file_.file,
            content_type=file_.content_type,
            size=file_.size,
            task_id=task_id,
            file_id=file_id,
            s3_metadata={
                "info": json.dumps(
                    {
                        "user_id": str(user_id),
                        "chat_id": str(chat_id),
                        "filename": file_.filename,
                    }
                ),
                "task_op": operation,
            },
        )
    return {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "file_ids": [str(file_id) for file_id in file_ids],
        "task_ids": [str(task_id) for task_id in task_ids],
    }


def s3_batch_delete(
    minio_client: Minio,
    user_id: UUID,
    chat_id: UUID,
    file_ids: List[UUID],
):
    for file_id in file_ids:
        s3_dump_file(
            minio_client,
            file_io=BytesIO(b""),
            size=0,
            content_type="application/octet-stream",
            task_id=str(uuid4()),
            file_id=file_id,
            s3_metadata={
                "info": json.dumps(
                    {
                        "user_id": str(user_id),
                        "chat_id": str(chat_id),
                        "filename": "None",
                    }
                ),
                "task_op": str(ObjectOperation.DELETE),
            },
        )
