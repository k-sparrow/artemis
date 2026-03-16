# core
from typing import Annotated

# third party
from fastapi import Depends
from minio import Minio

# our code
from src.backend.controller.worker.config import settings

__all__ = [
    "get_s3_client",
    "s3_client_dependency",
]


def get_s3_client():
    return Minio(
        endpoint=settings.S3_ENDPOINT,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=settings.S3_SECURE,
    )


s3_client_dependency = Annotated[Minio, Depends(get_s3_client)]
