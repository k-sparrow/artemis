from typing import Annotated

import httpx
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.backend.enterprise.intake.api.config import settings
from src.lib.backend.db.session import get_session, make_session_factory

storage_client: httpx.AsyncClient = httpx.AsyncClient(
    base_url=settings.STORAGE_SERVICE_URL
)

# Module-level factory; initialised once at import time.
# Tests may replace this with a test-scoped factory.
session_factory: async_sessionmaker[AsyncSession] = make_session_factory(
    settings.SQL_DB_URL
)


async def get_storage_client() -> httpx.AsyncClient:
    return storage_client


storage_client_dependency = Annotated[httpx.AsyncClient, Depends(get_storage_client)]


async def get_db_session():
    async for session in get_session(session_factory):
        yield session


db_session_dependency = Annotated[AsyncSession, Depends(get_db_session)]
