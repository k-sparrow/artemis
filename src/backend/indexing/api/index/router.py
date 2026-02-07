from typing import List

from fastapi import APIRouter, UploadFile, File
from langchain_core.documents import Document

from src.backend.indexing.api.index import service
from src.lib.backend.logging import get_logger


__all__ = [
    "router",
]


logger = get_logger("indexing.router")

router = APIRouter(tags=["Ingestion"])


@router.post(
    "/ingest",
    response_model=List[Document],
)
async def ingest_endpoint(file: UploadFile = File(...)) -> List[Document]:
    logger.info(
        "ingest_started", filename=file.filename, content_type=file.content_type
    )
    try:
        result = await service.ingest(file)
        logger.info(
            "ingest_completed",
            filename=file.filename,
            chunks=len(result),
            content_length=sum(len(doc.page_content) for doc in result),
        )
        return result
    except Exception as e:
        logger.error("ingest_failed", filename=file.filename, error=str(e))
        raise
