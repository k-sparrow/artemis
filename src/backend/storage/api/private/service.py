from uuid import UUID, uuid4
from typing import List

from fastapi import UploadFile
from minio import Minio

from src.backend.storage.api.private.exceptions import FilesAndFileIdsLengthMismatch
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
    if not file_ids:
        file_ids = [uuid4() for _ in range(0, len(files))]
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
