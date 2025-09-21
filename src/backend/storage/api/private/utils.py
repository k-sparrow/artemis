from typing import BinaryIO, Dict
from uuid import UUID

from minio import Minio

from src.backend.storage.api.config import storage_settings as settings


__all__ = [
    "s3_dump_file",
]


def s3_dump_file(
    minio_client: Minio,
    file_io: BinaryIO,
    content_type: str,
    size: int,
    file_id: UUID,
    task_id: UUID,
    s3_metadata: Dict[str, str] = {},
) -> None:
    file_io.seek(0)
    minio_client.put_object(
        bucket_name=settings.S3_VENUS_BUCKET,
        object_name=f"venus/private/{str(file_id)}",
        data=file_io,
        length=size,
        content_type=content_type,
        metadata={
            "file_id": str(file_id),
            "task_id": str(task_id),
            **s3_metadata,
        },
    )
