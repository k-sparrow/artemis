import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.backend.indexing.api.config import settings
from src.backend.indexing.api.dependencies import (
    get_vectorstore_handler_solved,
    VectorStoreHandler,
)
from src.lib.backend.logging import configure_logging, get_logger


__all__ = [
    "lifespan",
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("indexing")

    logger.info("lifespan_started")
    handler: VectorStoreHandler = await get_vectorstore_handler_solved()
    await handler.acreate()
    yield
    await handler.aclose()
    logger.info("lifespan_ended")
