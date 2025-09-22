import asyncio
from typing import Annotated

from fastapi import Depends
from minio import Minio

from src.backend.storage.api.config import storage_settings as settings

__all__ = [
    "get_minio_client",
    "get_minio_client_sync",
    "minio_client_dependency",
]


# Set up MinIO client instance
async def get_minio_client() -> Minio:
    return Minio(
        endpoint=settings.S3_ENDPOINT_URL,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=False,  # or None
    )


def get_minio_client_sync() -> Minio:
    return Minio(
        endpoint=settings.S3_ENDPOINT_URL,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=False,  # or None
    )


# FastAPI dependency for the MinIO client
minio_client_dependency = Annotated[
    Minio,
    Depends(get_minio_client),
]
