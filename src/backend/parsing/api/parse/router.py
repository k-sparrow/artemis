import json
import uuid
from typing import Annotated, Optional

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
    build_pages,
    chunk_items_to_parsed,
)
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.contract import BlobRef
from src.lib.core.ingestion.types import ParseArtifact

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
    obj_id: str


class ChunkSubmitRequest(BaseModel):
    obj_id: str


class ChunkSubmitResult(BaseModel):
    chunking_task_id: str
    obj_id: str


class ChunkFinalizeRequest(BaseModel):
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
    file: Optional[UploadFile] = File(None),
    source_ref: Optional[str] = Form(None),
    filename: Optional[str] = Form(None),
    content_type: Optional[str] = Form(None),
    metadata: _JsonDict = "{}",
) -> SubmitResult:
    """Queue a document conversion job on docling-serve and return immediately.

    Large PDFs are fanned out into page slices and converted concurrently
    server-side by docling-serve's Ray engine (see TODOs.md Epic 21), so no
    client-side splitting is needed. ``source_ref`` inputs are handed to
    docling-serve as an S3 source — it fetches the object directly from
    MinIO/S3 itself, so this service never downloads the bytes into its own
    memory. ``file`` (inline multipart) inputs are forwarded as-is.

    Poll ``GET /v1/parse/status/{task_id}`` until "success" or "failure",
    then call ``POST /v1/parse/resolve``.
    """
    if (file is None) == (source_ref is None):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="provide exactly one of 'file' or 'source_ref'",
        )

    obj_id = metadata["obj_id"]

    if file is not None:
        content = await file.read()
        in_name = filename or file.filename or "upload"
        in_type = content_type or file.content_type or "application/octet-stream"
        logger.info("submit_started", obj_id=obj_id, filename=in_name, mode="file")
        task_id = await docling_client.submit_file(
            content=content,
            filename=in_name,
            content_type=in_type,
            timeout=120.0,
        )
    else:
        ref = BlobRef.model_validate_json(source_ref)
        logger.info(
            "submit_started",
            obj_id=obj_id,
            filename=filename or ref.key.split("/")[-1],
            mode="source",
            bucket=ref.bucket,
            key=ref.key,
        )
        task_id = await docling_client.submit_source(
            s3_endpoint=settings.S3_ENDPOINT,
            s3_access_key=settings.S3_ACCESS_KEY,
            s3_secret_key=settings.S3_SECRET_KEY,
            s3_secure=settings.S3_SECURE,
            bucket=ref.bucket,
            key=ref.key,
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
    """Download a completed conversion, cache it, and derive its pages.

    Must only be called after /v1/parse/status/{task_id} returns "success".
    Writes the lossless DoclingDocument JSON to the replay cache — used both
    for citation/re-chunk lookups and as the source /v1/chunk/submit reads
    directly from S3 — and derives page-level Markdown parents right away,
    while the DoclingDocument is already in hand. Chunking is a fully
    separate stage from here on (Epic 21): this endpoint never submits a
    chunk job itself.
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

    pages = build_pages(dl_doc, uuid.UUID(obj_id))
    await replay_store.aput(
        service.pages_key(obj_id),
        service.encode_pages(pages),
        content_type="application/json",
    )
    logger.info("pages_cached", obj_id=obj_id, num_pages=len(pages))

    return ResolveResult(obj_id=obj_id)


# ---------------------------------------------------------------------------
# Chunking endpoints — a fully decoupled stage from parsing/conversion above
# (Epic 21). Named /v1/chunk/* rather than nested under /v1/parse/ so the
# path doesn't imply chunking is owned by "parsing" — it isn't, this service
# just happens to be the sole Docling adapter for now. The worker orchestrates
# *when* chunking runs as its own chain stage; this service still owns every
# direct interaction with docling-serve, same as conversion above.
# ---------------------------------------------------------------------------


@router.post("/v1/chunk/submit", response_model=ChunkSubmitResult)
async def chunk_submit_endpoint(
    request: ChunkSubmitRequest,
    docling_client: docling_client_dependency,
) -> ChunkSubmitResult:
    """Submit the replay-cached DoclingDocument for hybrid chunking.

    Must only be called after /v1/parse/resolve has written the replay cache
    for this obj_id. docling-serve fetches the JSON directly from S3 — this
    service never re-reads or re-uploads it (mirrors /v1/parse/submit's
    source_ref path).

    Poll GET /v1/chunk/status/{task_id} until "success", then call
    POST /v1/chunk/finalize.
    """
    obj_id = request.obj_id
    chunking_task_id = await docling_client.submit_chunk_source(
        s3_endpoint=settings.S3_ENDPOINT,
        s3_access_key=settings.S3_ACCESS_KEY,
        s3_secret_key=settings.S3_SECRET_KEY,
        s3_secure=settings.S3_SECURE,
        bucket=settings.REPLAY_CACHE_BUCKET,
        key=service.replay_key(obj_id),
        timeout=120.0,
    )
    logger.info("chunk_submit_queued", obj_id=obj_id, chunking_task_id=chunking_task_id)
    return ChunkSubmitResult(chunking_task_id=chunking_task_id, obj_id=obj_id)


@router.get("/v1/chunk/status/{task_id}", response_model=ParseStatus)
async def chunk_status_endpoint(
    task_id: str,
    docling_client: docling_client_dependency,
) -> ParseStatus:
    """Proxy a single non-blocking status check to docling-serve for a chunk task.

    Identical proxy logic to /v1/parse/status — docling-serve uses one task
    namespace for every task type regardless of origin — this route exists so
    polling a chunk task doesn't read as polling a parse task.
    """
    return await docling_client.get_status(
        task_id, timeout=settings.DOCLING_STATUS_TIMEOUT
    )


@router.post("/v1/chunk/finalize", response_model=BlobRef)
async def chunk_finalize_endpoint(
    request: ChunkFinalizeRequest,
    docling_client: docling_client_dependency,
    blob_store: blob_store_factory_dependency,
) -> BlobRef:
    """Fetch chunk result, merge with the cached pages, write the final ParseArtifact.

    Must only be called after /v1/chunk/status/{chunking_task_id} returns
    "success". Reads back the pages /v1/parse/resolve already derived —
    finalizing chunks never re-fetches or re-parses the DoclingDocument itself.
    """
    obj_id_str = request.obj_id
    obj_id = uuid.UUID(obj_id_str)
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)

    chunks = await docling_client.fetch_chunk_result(
        request.chunking_task_id,
        timeout=settings.DOCLING_FINALIZE_TIMEOUT,
    )
    parsed_chunks = chunk_items_to_parsed(chunks, obj_id)

    pages_bytes = await replay_store.aget(service.pages_key(obj_id_str))
    pages = service.decode_pages(pages_bytes)

    artifact = ParseArtifact(pages=pages, chunks=parsed_chunks)

    key = service.artifact_key(obj_id_str)
    await blob_store(settings.PARSED_ARTIFACTS_BUCKET).aput(
        key, service.encode_artifact(artifact), content_type="application/json"
    )
    logger.info(
        "chunk_finalize_completed",
        obj_id=obj_id_str,
        num_pages=len(artifact.pages),
        num_chunks=len(artifact.chunks),
        key=key,
    )
    return BlobRef(bucket=settings.PARSED_ARTIFACTS_BUCKET, key=key)
