from typing import Annotated

from fastapi import Depends
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.backend.storage.api.config import settings
from src.lib.backend.db.session import get_session, make_session_factory

__all__ = [
    "get_minio_client",
    "get_minio_client_sync",
    "get_db_session",
    "minio_client_dependency",
    "session_factory",
    "db_session_dependency",
]


# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------


async def get_minio_client() -> Minio:
    return Minio(
        endpoint=settings.S3_ENDPOINT_URL,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=False,
    )


def get_minio_client_sync() -> Minio:
    return Minio(
        endpoint=settings.S3_ENDPOINT_URL,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=False,
    )


minio_client_dependency = Annotated[Minio, Depends(get_minio_client)]


# ---------------------------------------------------------------------------
# Postgres (async SQLAlchemy)
# ---------------------------------------------------------------------------

# Module-level factory; initialised once at import time.
# Tests may replace this with a test-scoped factory.
session_factory: async_sessionmaker[AsyncSession] = make_session_factory(
    settings.SQL_DB_URL
)


async def get_db_session():
    async for session in get_session(session_factory):
        yield session


db_session_dependency = Annotated[AsyncSession, Depends(get_db_session)]
