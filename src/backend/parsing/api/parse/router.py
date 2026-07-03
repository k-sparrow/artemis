import asyncio
import json
import uuid
from typing import Annotated, Literal, Optional

from docling.datamodel.document import DoclingDocument
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, BeforeValidator

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    blob_store_factory_dependency,
    docling_client_dependency,
    loader_factory_dependency,
    s3_client_dependency,
)
from src.backend.parsing.api.parse import service
from src.backend.parsing.lib.artifact import (
    ParseStatus,
    build_artifact,
    chunk_items_to_parsed,
)
from src.backend.parsing.lib.sharding import pdf_page_count, split_pdf_shards
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
    mode: Literal["single", "batch"]
    obj_id: str
    scratch_prefix: Optional[str] = None
    shard_count: Optional[int] = None


class ResolveRequest(BaseModel):
    parsing_task_id: str
    mode: Literal["single", "batch"]
    obj_id: str
    scratch_prefix: Optional[str] = None
    shard_count: Optional[int] = None


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

    Large PDFs (> DOCLING_SHARD_TRIGGER_PAGES pages) are split into shards,
    uploaded to the scratch bucket, and submitted as a batch S3 conversion.
    All other documents are submitted as a single async file conversion.

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

    is_pdf = (in_type or "").lower() == "application/pdf" or in_name.lower().endswith(
        ".pdf"
    )

    if is_pdf and pdf_page_count(content) > settings.DOCLING_SHARD_TRIGGER_PAGES:
        shards = split_pdf_shards(content, settings.DOCLING_SHARD_PAGE_LIMIT)
        scratch = blob_store(settings.DOCLING_SCRATCH_BUCKET)
        shard_prefix = f"{obj_id}/pages/"
        for i, shard_bytes in enumerate(shards):
            key = f"{shard_prefix}shard-{i:04d}.pdf"
            await scratch.aput(key, shard_bytes, content_type="application/pdf")

        logger.info(
            "submit_batch_started",
            obj_id=obj_id,
            shard_count=len(shards),
            shard_prefix=shard_prefix,
        )
        task_id = await docling_client.submit_batch(
            s3_endpoint=settings.S3_ENDPOINT,
            s3_access_key=settings.S3_ACCESS_KEY,
            s3_secret_key=settings.S3_SECRET_KEY,
            s3_secure=settings.S3_SECURE,
            src_bucket=settings.DOCLING_SCRATCH_BUCKET,
            src_prefix=shard_prefix,
            dst_bucket=settings.DOCLING_SCRATCH_BUCKET,
            dst_prefix=f"{obj_id}/results/",
            timeout=120.0,
        )
        logger.info("submit_batch_queued", obj_id=obj_id, parsing_task_id=task_id)
        return SubmitResult(
            parsing_task_id=task_id,
            mode="batch",
            obj_id=obj_id,
            scratch_prefix=f"{obj_id}/",
            shard_count=len(shards),
        )

    logger.info("submit_single_started", obj_id=obj_id, filename=in_name)
    task_id = await docling_client.submit_file(
        content=content,
        filename=in_name,
        content_type=in_type,
        timeout=120.0,
    )
    logger.info("submit_single_queued", obj_id=obj_id, parsing_task_id=task_id)
    return SubmitResult(
        parsing_task_id=task_id,
        mode="single",
        obj_id=obj_id,
    )


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
    s3: s3_client_dependency,
) -> ResolveResult:
    """Download a completed conversion, concatenate shards if needed, submit chunk job.

    Must only be called after /v1/parse/status/{task_id} returns "success".
    Writes the lossless DoclingDocument JSON to the private replay cache so that
    /v1/parse/finalize can extract page parents without passing large blobs.
    Scratch objects are deleted after a successful chunk submission (batch mode).
    """
    obj_id = request.obj_id
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)
    scratch_store = blob_store(settings.DOCLING_SCRATCH_BUCKET)

    if request.mode == "single":
        dl_doc = await docling_client.fetch_conversion_result(
            request.parsing_task_id,
            timeout=settings.DOCLING_RESOLVE_TIMEOUT,
        )
    else:
        results_prefix = f"{obj_id}/results/"
        keys = sorted(
            obj.object_name
            for obj in s3.list_objects(
                settings.DOCLING_SCRATCH_BUCKET, prefix=results_prefix, recursive=True
            )
            if obj.object_name
        )
        if not keys:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"no batch results found at {results_prefix}",
            )

        shard_docs: list[DoclingDocument] = []
        for key in keys:
            raw = await scratch_store.aget(key)
            shard_docs.append(DoclingDocument.model_validate_json(raw))

        dl_doc = DoclingDocument.concatenate(shard_docs)
        logger.info(
            "batch_shards_concatenated",
            obj_id=obj_id,
            shard_count=len(shard_docs),
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

    if request.mode == "batch" and request.scratch_prefix:
        await _delete_scratch_prefix(scratch_store, request.scratch_prefix, s3)

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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _delete_scratch_prefix(scratch_store, prefix: str, s3_client) -> None:
    """Best-effort deletion of all scratch objects under prefix."""
    try:
        keys = [
            obj.object_name
            for obj in s3_client.list_objects(
                settings.DOCLING_SCRATCH_BUCKET, prefix=prefix, recursive=True
            )
            if obj.object_name
        ]
        await asyncio.gather(*(scratch_store.adelete(k) for k in keys))
        logger.info("scratch_cleaned", prefix=prefix, count=len(keys))
    except Exception as exc:
        logger.warning("scratch_cleanup_failed", prefix=prefix, error=str(exc))
