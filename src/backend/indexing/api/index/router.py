from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from opentelemetry.trace import Status, StatusCode, get_current_span

from src.backend.indexing.api.dependencies import (
    blob_store_factory_dependency,
    pipeline_dependency,
)
from src.backend.indexing.api.index import service
from src.backend.indexing.api.index.service import IngestRequest
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.exceptions import DocumentProcessingException
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
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="provide exactly one of 'artifact_ref' or 'chunks'",
        )

    if body.artifact_ref is not None:
        reader = blob_store(body.artifact_ref.bucket)
        artifact = service.decode_artifact(await reader.aget(body.artifact_ref.key))
        chunks = artifact.chunks
        pages = artifact.pages
    else:
        # Inline chunks carry no page parents; indexing still stamps parent_id
        # (chunks fall back to their object's first page when one exists).
        chunks = body.chunks
        pages = []

    # Stamped early (before the pipeline/RecordManager call, not just on
    # failure) so any child span the RecordManager's own vectorstore delete
    # call creates underneath this one — including a scoped_full cleanup —
    # is traceable back to the object being ingested when it fires.
    obj_id = (
        str(pages[0].obj_id) if pages else (str(chunks[0].obj_id) if chunks else None)
    )
    span = get_current_span()
    if span.is_recording():
        span.set_attribute("artemis.namespace_id", str(namespace))
        if obj_id is not None:
            span.set_attribute("artemis.obj_id", obj_id)
        if group_id is not None:
            span.set_attribute("artemis.group_id", group_id)

    def _fail(exc: DocumentProcessingException) -> None:
        if span.is_recording():
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, exc.message))
        raise exc

    if body.artifact_ref is not None:
        if not pages:
            _fail(
                DocumentProcessingException(
                    f"parsing produced zero pages for obj_id={obj_id} "
                    f"in namespace={namespace}"
                )
            )
        if not chunks:
            # By this point the parsing service's own chunk-empty fallback
            # (Epic 21.8/21.11 — see chunk_finalize_endpoint) has already
            # run, so a genuinely empty chunk list here means something
            # upstream is broken, not a case to re-split for. No fallback
            # logic lives in this service — it has no chunking-strategy
            # knowledge by design.
            _fail(
                DocumentProcessingException(
                    f"chunking produced zero chunks for obj_id={obj_id} "
                    f"in namespace={namespace} (unexpected: the parsing "
                    "service is expected to guarantee a non-empty artifact)"
                )
            )
    elif not chunks:
        # Inline mode carries no pages, so there's no fallback split to try —
        # and an empty list here is otherwise indistinguishable from
        # PagedInput's own namespace-wipe signal (both pages and chunks
        # empty), which ParentPagePipeline.aprocess() reads as "clear
        # everything". A caller passing chunks=[] must never reach that
        # path by accident.
        _fail(
            DocumentProcessingException(
                f"inline chunks must be non-empty for namespace={namespace} "
                "(an empty list is reserved for DELETE /ingest's namespace wipe)"
            )
        )

    logger.info("ingest_started", num_chunks=len(chunks), num_pages=len(pages))
    try:
        result = await service.a_index_and_ingest(
            chunks, pages, pipeline, namespace, group_id
        )
        logger.info(
            "ingest_completed",
            num_added=result.num_added,
            num_skipped=result.num_skipped,
        )
        return result
    except Exception as e:
        logger.error("ingest_failed", error=str(e))
        if span.is_recording():
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
        raise
