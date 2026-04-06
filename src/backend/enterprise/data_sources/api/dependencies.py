from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.backend.enterprise.data_sources.api.config import settings
from src.lib.backend.db.session import get_session, make_session_factory

__all__ = [
    "get_db_session",
    "session_factory",
    "db_session_dependency",
]

# Module-level factory; initialised once at import time.
# Tests may replace this with a test-scoped factory.
session_factory: async_sessionmaker[AsyncSession] = make_session_factory(
    settings.SQL_DB_URL
)


async def get_db_session():
    async for session in get_session(session_factory):
        yield session


db_session_dependency = Annotated[AsyncSession, Depends(get_db_session)]
