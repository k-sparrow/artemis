from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter

from src.backend.indexing.api.dependencies import pipeline_dependency
from src.backend.indexing.api.index import service
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.types import ParsedChunk, UpsertResult


__all__ = [
    "router",
]


logger = get_logger("indexing.router")

router = APIRouter(tags=["Ingestion"])


@router.delete("/ingest", status_code=204)
async def delete_endpoint(
    pipeline: pipeline_dependency,
    namespace: UUID,
    source: Optional[str] = None,
) -> None:
    logger.info("delete_started", namespace=str(namespace), source=source)
    try:
        await service.a_delete(namespace, pipeline, source)
        logger.info("delete_completed", namespace=str(namespace), source=source)
    except Exception as e:
        logger.error("delete_failed", error=str(e))
        raise


@router.post("/ingest", response_model=UpsertResult)
async def ingest_endpoint(
    chunks: List[ParsedChunk],
    pipeline: pipeline_dependency,
    namespace: UUID,
) -> UpsertResult:
    logger.info("ingest_started", num_chunks=len(chunks))
    try:
        result = await service.a_index_and_ingest(chunks, pipeline, namespace)
        logger.info(
            "ingest_completed",
            num_added=result.num_added,
            num_skipped=result.num_skipped,
        )
        return result
    except Exception as e:
        logger.error("ingest_failed", error=str(e))
        raise
