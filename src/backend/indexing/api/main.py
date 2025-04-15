from contextlib import asynccontextmanager
from typing import List, AsyncIterator
from logging import getLogger, INFO


from fastapi import FastAPI, UploadFile, File
from langchain_core.documents import Document

import src.backend.indexing.api.service as service
from src.backend.indexing.api.dependencies import (
    get_vectorstore_handler,
    VectorStoreHandler,
    get_vectorstore_handler_solved,
)


logger = getLogger(__name__)
logger.setLevel(INFO)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Starting FastAPI setup...")
    handler: VectorStoreHandler = await get_vectorstore_handler_solved()
    await handler.acreate()
    yield
    await handler.aclose()
    logger.info("Tearing down application...")


app = FastAPI(
    title="Basic File Ingestion with Celery",
    lifespan=lifespan,
)


@app.post(
    "/ingest",
    response_model=List[Document],
)
async def ingest_endpoint(file: UploadFile = File(...)) -> List[Document]:
    return await service.ingest(file)
