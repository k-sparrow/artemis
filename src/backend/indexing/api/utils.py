import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import cohere
from fastapi import FastAPI

from src.backend.indexing.api.config import settings
from src.backend.indexing.api.dependencies import (
    build_page_byte_store,
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

    logger.info("lifespan_started", retrieval_mode=settings.RETRIEVAL_MODE)

    logger.info("vectorstore_handler_init_started")
    handler: VectorStoreHandler = await get_vectorstore_handler_solved()
    logger.info("vectorstore_handler_init_done")

    logger.info("vectorstore_create_started")
    vs = await handler.acreate()
    logger.info("vectorstore_create_done")

    retrieval_adapter = SimpleVectorStoreRetrieverAdapter(vs)

    # Parent-page doc store — always built (indexing caches pages for every
    # object). ensure_bucket runs once here so the first ingest can PUT pages;
    # the same store backs per-request parent-page dereference on /retrieve.
    logger.info("page_store_init_started", bucket=settings.PAGE_BUCKET)
    page_byte_store = build_page_byte_store()
    await page_byte_store.ensure_bucket()
    logger.info("page_store_init_done")

    reranker: Optional[CohereRerank] = None
    if settings.COLBERT_RERANKER_URL:
        logger.info("reranker_init_started", model=settings.COLBERT_MODEL_NAME)
        cohere_client = cohere.ClientV2(
            api_key="not-needed", base_url=settings.COLBERT_RERANKER_URL
        )
        reranker = CohereRerank(
            model=settings.COLBERT_MODEL_NAME,
            client=cohere_client,
            max_tokens_per_doc=settings.COLBERT_MAX_TOKENS_PER_DOC,
        )
        logger.info("reranker_init_done")

    retrieve_service.initialize(
        retrieval_adapter,
        reranker,
        settings.RETRIEVE_CANDIDATES_MULTIPLIER,
        byte_store=page_byte_store,
    )
    logger.info("lifespan_ready")

    yield
    await handler.aclose()
    logger.info("lifespan_ended")
