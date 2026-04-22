import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.backend.enterprise.intake.api.config import settings
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
    yield
    logger.info("lifespan_ended")
