import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from src.backend.enterprise.data_sources.api.config import settings
from src.backend.enterprise.data_sources.api.models import Base
from src.lib.backend.logging import configure_logging, get_logger
from src.lib.backend.telemetry import setup_telemetry


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_telemetry("backend-enterprise-data-sources")
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("data_sources")
    logger.info("lifespan_started")

    # Create Postgres tables (idempotent; Alembic handles this in production).
    engine = create_async_engine(settings.SQL_DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    logger.info(
        "lifespan_ready",
        storage_url=settings.STORAGE_SERVICE_URL,
        kafka_connect_url=settings.KAFKA_CONNECT_URL,
    )
    yield

    logger.info("lifespan_ended")
