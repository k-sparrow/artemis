from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from src.backend.indexing.api.dependencies import (
    blob_store_factory_dependency,
    pipeline_dependency,
)
from src.backend.indexing.api.index import service
from src.backend.indexing.api.index.service import IngestRequest
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.types import UpsertResult


__all__ = [
    "router",
]


logger = get_logger("indexing.router")

router = APIRouter(tags=["Ingestion"])


@router.delete("/ingest", status_code=204)
async def delete_endpoint(
    pipeline: pipeline_dependency,
    namespace: UUID,
    obj_id: Optional[str] = None,
) -> None:
    logger.info("delete_started", namespace=str(namespace), obj_id=obj_id)
    try:
        await service.a_delete(namespace, pipeline, obj_id)
        logger.info("delete_completed", namespace=str(namespace), obj_id=obj_id)
    except Exception as e:
        logger.error("delete_failed", error=str(e))
        raise


@router.post("/ingest", response_model=UpsertResult)
async def ingest_endpoint(
    body: IngestRequest,
    pipeline: pipeline_dependency,
    blob_store: blob_store_factory_dependency,
    namespace: UUID,
    group_id: Optional[str] = None,
) -> UpsertResult:
    """Index chunks, inline-or-reference.

    Supply exactly one of ``artifact_ref`` (a ``BlobRef`` pointing at the parse
    artifact in object storage — read here so the payload never crosses the
    wire) or ``chunks`` (inline, for standalone use / tests).
    """
    if (body.artifact_ref is None) == (body.chunks is None):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="provide exactly one of 'artifact_ref' or 'chunks'",
        )

    if body.artifact_ref is not None:
        reader = blob_store(body.artifact_ref.bucket)
        chunks = service.decode_artifact(await reader.aget(body.artifact_ref.key))
    else:
        chunks = body.chunks

    logger.info("ingest_started", num_chunks=len(chunks))
    try:
        result = await service.a_index_and_ingest(chunks, pipeline, namespace, group_id)
        logger.info(
            "ingest_completed",
            num_added=result.num_added,
            num_skipped=result.num_skipped,
        )
        return result
    except Exception as e:
        logger.error("ingest_failed", error=str(e))
        raise
