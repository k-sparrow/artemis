import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.backend.mcp.api import client
from src.backend.mcp.api.settings import settings
from src.lib.backend.logging import configure_logging, get_logger
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from src.lib.backend.telemetry import is_telemetry_enabled, setup_telemetry

__all__ = [
    "lifespan",
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if is_telemetry_enabled():
        setup_telemetry(
            "backend-mcp",
            FastAPIInstrumentor(),
            HTTPXClientInstrumentor(),
            SQLAlchemyInstrumentor(),
        )
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("mcp")
    logger.info("lifespan_started")
    yield
    await client.storage_client.aclose()
    await client.indexing_client.aclose()
    logger.info("lifespan_ended")
