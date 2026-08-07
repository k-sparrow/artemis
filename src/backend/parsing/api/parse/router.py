import json
import uuid
from typing import Annotated, Optional

from docling.datamodel.document import DoclingDocument
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, BeforeValidator

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    blob_store_factory_dependency,
    docling_client_dependency,
    loader_factory_dependency,
)
from src.backend.parsing.api.parse import service
from src.backend.parsing.lib.artifact import (
    ParseStatus,
    build_artifact,
    chunk_items_to_parsed,
)
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.contract import BlobRef

__all__ = ["router"]


logger = get_logger("parsing.router")

router = APIRouter(tags=["Parsing"])

# Form fields are always strings on the wire; this validator JSON-parses the
# raw string into dict[str, str] before Pydantic validates the type.
_JsonDict = Annotated[dict[str, str], BeforeValidator(json.loads), Form()]


# ---------------------------------------------------------------------------
# Request / response models for the new async parse endpoints
# ---------------------------------------------------------------------------


class SubmitResult(BaseModel):
    parsing_task_id: str
    obj_id: str


class ResolveRequest(BaseModel):
    parsing_task_id: str
    obj_id: str


class ResolveResult(BaseModel):
    chunking_task_id: str
    obj_id: str


class FinalizeRequest(BaseModel):
    chunking_task_id: str
    obj_id: str
    metadata: dict[str, str]


# ---------------------------------------------------------------------------
# Existing synchronous endpoint (retained; becomes dead code after migration)
# ---------------------------------------------------------------------------


@router.post("/v1/parse", response_model=BlobRef)
async def parse_endpoint(
    loader_factory: loader_factory_dependency,
    blob_store: blob_store_factory_dependency,
    file: Optional[UploadFile] = File(None),
    source_ref: Optional[str] = Form(None),
    filename: Optional[str] = Form(None),
    content_type: Optional[str] = Form(None),
    metadata: _JsonDict = "{}",
) -> BlobRef:
    """Parse a document and write the artifact to object storage (claim-check).

    Input is inline-or-reference: supply exactly one of ``file`` (multipart
    bytes) or ``source_ref`` (a ``BlobRef`` JSON pointing at input bytes in
    object storage). The artifact is written under ``parse/{obj_id}.json`` and
    its location is returned as a ``BlobRef`` — the payload never crosses the wire.
    """
    if (file is None) == (source_ref is None):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="provide exactly one of 'file' or 'source_ref'",
        )

    if file is not None:
        content = await file.read()
        in_name = filename or file.filename
        in_type = content_type or file.content_type
        logger.info("parse_started", source="inline", filename=in_name)
    else:
        ref = BlobRef.model_validate_json(source_ref)
        content = await blob_store(ref.bucket).aget(ref.key)
        in_name, in_type = filename, content_type
        logger.info(
            "parse_started",
            source="ref",
            filename=in_name,
            bucket=ref.bucket,
            key=ref.key,
        )

    try:
        artifact, replay = await service.a_parse(
            content=content,
            filename=in_name,
            content_type=in_type,
            loader_factory=loader_factory,
            metadata=metadata,
        )
    except Exception as e:
        logger.error("parse_failed", filename=in_name, error=str(e))
        raise

    obj_id = metadata["obj_id"]

    # Private replay cache (lossless DoclingDocument) — best-effort, never in the
    # contract; persist before the artifact so a write failure surfaces here.
    await blob_store(settings.REPLAY_CACHE_BUCKET).aput(
        service.replay_key(obj_id), replay, content_type="application/json"
    )

    key = service.artifact_key(obj_id)
    await blob_store(settings.PARSED_ARTIFACTS_BUCKET).aput(
        key, service.encode_artifact(artifact), content_type="application/json"
    )
    logger.info(
        "parse_completed",
        filename=in_name,
        num_pages=len(artifact.pages),
        num_chunks=len(artifact.chunks),
        key=key,
    )
    return BlobRef(bucket=settings.PARSED_ARTIFACTS_BUCKET, key=key)


# ---------------------------------------------------------------------------
# Async parse endpoints — used by the new Celery parse sub-chain
# ---------------------------------------------------------------------------


@router.post("/v1/parse/submit", response_model=SubmitResult)
async def submit_endpoint(
    docling_client: docling_client_dependency,
    blob_store: blob_store_factory_dependency,
    file: Optional[UploadFile] = File(None),
    source_ref: Optional[str] = Form(None),
    filename: Optional[str] = Form(None),
    content_type: Optional[str] = Form(None),
    metadata: _JsonDict = "{}",
) -> SubmitResult:
    """Queue a document conversion job on docling-serve and return immediately.

    Every document is submitted as a single async file conversion — large PDFs
    are fanned out into page slices and converted concurrently server-side by
    docling-serve's Ray engine (see TODOs.md Epic 21), so no client-side
    splitting is needed.

    Poll ``GET /v1/parse/status/{task_id}`` until "success" or "failure",
    then call ``POST /v1/parse/resolve``.
    """
    if (file is None) == (source_ref is None):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="provide exactly one of 'file' or 'source_ref'",
        )

    if file is not None:
        content = await file.read()
        in_name = filename or file.filename or "upload"
        in_type = content_type or file.content_type or "application/octet-stream"
    else:
        ref = BlobRef.model_validate_json(source_ref)
        content = await blob_store(ref.bucket).aget(ref.key)
        in_name = filename or ref.key.split("/")[-1]
        in_type = content_type or "application/octet-stream"

    obj_id = metadata["obj_id"]

    logger.info("submit_started", obj_id=obj_id, filename=in_name)
    task_id = await docling_client.submit_file(
        content=content,
        filename=in_name,
        content_type=in_type,
        timeout=120.0,
    )
    logger.info("submit_queued", obj_id=obj_id, parsing_task_id=task_id)
    return SubmitResult(parsing_task_id=task_id, obj_id=obj_id)


@router.get("/v1/parse/status/{task_id}", response_model=ParseStatus)
async def status_endpoint(
    task_id: str,
    docling_client: docling_client_dependency,
) -> ParseStatus:
    """Proxy a single non-blocking status check to docling-serve.

    Works for both conversion task_ids (from /v1/parse/submit) and chunk
    task_ids (from /v1/parse/resolve) — docling-serve uses a unified task
    namespace for all task types.
    """
    return await docling_client.get_status(
        task_id, timeout=settings.DOCLING_STATUS_TIMEOUT
    )


@router.post("/v1/parse/resolve", response_model=ResolveResult)
async def resolve_endpoint(
    request: ResolveRequest,
    docling_client: docling_client_dependency,
    blob_store: blob_store_factory_dependency,
) -> ResolveResult:
    """Download a completed conversion and submit a chunk job.

    Must only be called after /v1/parse/status/{task_id} returns "success".
    Writes the lossless DoclingDocument JSON to the private replay cache so that
    /v1/parse/finalize can extract page parents without passing large blobs.
    """
    obj_id = request.obj_id
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)

    dl_doc = await docling_client.fetch_conversion_result(
        request.parsing_task_id,
        timeout=settings.DOCLING_RESOLVE_TIMEOUT,
    )

    replay_bytes = dl_doc.model_dump_json().encode()
    await replay_store.aput(
        service.replay_key(obj_id), replay_bytes, content_type="application/json"
    )
    logger.info("replay_cache_written", obj_id=obj_id)

    chunking_task_id = await docling_client.submit_chunk(
        replay_bytes,
        timeout=120.0,
    )
    logger.info("chunk_job_submitted", obj_id=obj_id, chunking_task_id=chunking_task_id)

    return ResolveResult(chunking_task_id=chunking_task_id, obj_id=obj_id)


@router.post("/v1/parse/finalize", response_model=BlobRef)
async def finalize_endpoint(
    request: FinalizeRequest,
    docling_client: docling_client_dependency,
    blob_store: blob_store_factory_dependency,
) -> BlobRef:
    """Fetch chunk result, build ParseArtifact from replay cache, write to MinIO.

    Must only be called after /v1/parse/status/{chunking_task_id} returns "success".
    The replay cache (written by /v1/parse/resolve) is used for page extraction
    so the full DoclingDocument never crosses the wire in the response body.
    """
    obj_id_str = request.obj_id
    obj_id = uuid.UUID(obj_id_str)

    chunks = await docling_client.fetch_chunk_result(
        request.chunking_task_id,
        timeout=settings.DOCLING_FINALIZE_TIMEOUT,
    )

    replay_bytes = await blob_store(settings.REPLAY_CACHE_BUCKET).aget(
        service.replay_key(obj_id_str)
    )
    dl_doc = DoclingDocument.model_validate_json(replay_bytes)

    parsed_chunks = chunk_items_to_parsed(chunks, obj_id)
    artifact = build_artifact(parsed_chunks, dl_doc, obj_id)

    key = service.artifact_key(obj_id_str)
    await blob_store(settings.PARSED_ARTIFACTS_BUCKET).aput(
        key, service.encode_artifact(artifact), content_type="application/json"
    )
    logger.info(
        "finalize_completed",
        obj_id=obj_id_str,
        num_pages=len(artifact.pages),
        num_chunks=len(artifact.chunks),
        key=key,
    )
    return BlobRef(bucket=settings.PARSED_ARTIFACTS_BUCKET, key=key)
