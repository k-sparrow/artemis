from typing import List

from fastapi import APIRouter, UploadFile, File

from src.backend.indexing.api.dependencies import (
    loader_factory_dependency,
    pipeline_dependency,
)
from src.backend.indexing.api.index import service
from src.lib.backend.logging import get_logger


__all__ = [
    "router",
]


logger = get_logger("indexing.router")

router = APIRouter(tags=["Ingestion"])


@router.post(
    "/ingest",
    response_model=List[str],
)
async def ingest_endpoint(
    loader_factory: loader_factory_dependency,
    pipeline: pipeline_dependency,
    file: UploadFile = File(...),
) -> List[str]:
    logger.info(
        "ingest_started", filename=file.filename, content_type=file.content_type
    )
    try:
        result = await service.ingest(file, loader_factory, pipeline)
        logger.info(
            "ingest_completed",
            filename=file.filename,
            upserted_ids=len(result),
        )
        return result
    except Exception as e:
        logger.error("ingest_failed", filename=file.filename, error=str(e))
        raise
