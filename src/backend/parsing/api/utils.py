import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.backend.parsing.api.config import settings
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
            "backend-parsing",
            FastAPIInstrumentor(),
            HTTPXClientInstrumentor(),
            SQLAlchemyInstrumentor(),
        )
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("parsing")
    logger.info("lifespan_started")
    yield
    logger.info("lifespan_ended")
