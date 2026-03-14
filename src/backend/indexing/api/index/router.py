from uuid import UUID

from fastapi import APIRouter, UploadFile, File

from src.backend.indexing.api.dependencies import (
    loader_factory_dependency,
    pipeline_dependency,
)
from src.backend.indexing.api.index import service
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.types import UpsertResult


__all__ = [
    "router",
]


logger = get_logger("indexing.router")

router = APIRouter(tags=["Ingestion"])


@router.post(
    "/ingest",
    response_model=UpsertResult,
)
async def ingest_endpoint(
    loader_factory: loader_factory_dependency,
    pipeline: pipeline_dependency,
    namespace: UUID,
    file: UploadFile = File(...),
) -> UpsertResult:
    logger.info(
        "ingest_started", filename=file.filename, content_type=file.content_type
    )
    try:
        result = await service.a_index_and_ingest(
            file, loader_factory, pipeline, namespace
        )
        logger.info(
            "ingest_completed",
            filename=file.filename,
            num_added=result.num_added,
            num_skipped=result.num_skipped,
        )
        return result
    except Exception as e:
        logger.error("ingest_failed", filename=file.filename, error=str(e))
        raise
