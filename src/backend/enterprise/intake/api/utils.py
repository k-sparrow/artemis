import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from src.backend.enterprise.intake.api.config import settings
from src.backend.enterprise.intake.api.intake.models import Base
from src.lib.backend.logging import configure_logging, get_logger


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("enterprise_intake")
    logger.info("lifespan_started", storage_url=settings.STORAGE_SERVICE_URL)

    # Create Postgres tables (idempotent; Alembic handles this in production).
    engine = create_async_engine(settings.SQL_DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    yield
    logger.info("lifespan_ended")
