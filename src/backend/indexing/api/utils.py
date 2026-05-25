import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import cohere
from fastapi import FastAPI

from src.backend.indexing.api.config import settings
from src.backend.indexing.api.dependencies import (
    get_vectorstore_handler_solved,
    VectorStoreHandler,
)
from src.backend.indexing.api.retrieve import service as retrieve_service
from src.lib.backend.logging import configure_logging, get_logger
from src.lib.core.adapters.document_compressors.cohere import CohereRerank
from src.lib.core.retrieval.adapters.simple import SimpleVectorStoreRetrieverAdapter


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
    vs = await handler.acreate()

    retrieval_adapter = SimpleVectorStoreRetrieverAdapter(vs)

    reranker: Optional[CohereRerank] = None
    if settings.COLBERT_RERANKER_URL:
        cohere_client = cohere.ClientV2(
            api_key="not-needed", base_url=settings.COLBERT_RERANKER_URL
        )
        reranker = CohereRerank(model=settings.COLBERT_MODEL_NAME, client=cohere_client)

    retrieve_service.initialize(
        retrieval_adapter, reranker, settings.RETRIEVE_CANDIDATES_MULTIPLIER
    )

    yield
    await handler.aclose()
    logger.info("lifespan_ended")
