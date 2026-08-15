import json
import uuid
from functools import wraps
from typing import Annotated, Literal, Optional

import httpx
from docling.datamodel.document import DoclingDocument
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from minio import Minio
from pydantic import BaseModel, BeforeValidator

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    blob_store_factory_dependency,
    docling_client_dependency,
    s3_client_dependency,
)
from src.backend.parsing.api.parse import service
from src.backend.parsing.lib.artifact import (
    ParseStatus,
    build_pages,
    chunk_items_to_parsed,
)
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.contract import BlobRef
from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamServiceException,
)
from src.lib.core.ingestion.types import ParseArtifact

__all__ = ["router"]


logger = get_logger("parsing.router")

router = APIRouter(tags=["Parsing"])

# Form fields are always strings on the wire; this validator JSON-parses the
# raw string into dict[str, str] before Pydantic validates the type.
_JsonDict = Annotated[dict[str, str], BeforeValidator(json.loads), Form()]


def _translate_docling_errors(fn):
    """Map a docling-serve HTTP failure to this service's domain exceptions.

    ``DoclingParseClient`` is a thin httpx facade — it raises raw httpx
    errors (see its module docstring) and leaves translation to the caller.
    Without this, an unreachable/erroring docling-serve surfaced as a bare
    500 here, which the worker's retry logic (branches on
    ``status_code >= 500``, see controller/worker/tasks.py) can't distinguish
    from a permanent failure.

    Connection failures, timeouts, and docling-serve 5xx are upstream
    problems — mapped to 503 (``UpstreamServiceException``) so the worker
    retries. A docling-serve 4xx means *this* request was rejected (e.g. a
    malformed submission) — mapped to 400 (``DocumentProcessingException``)
    so the worker treats it as permanent instead of retrying something that
    will never succeed. Content-level failures (e.g. an unsupported file
    *format*) don't hit this path at all — docling-serve reports those via
    task status ("failure"), not an HTTP error; see
    ``DoclingParseClient.get_status``.
    """

    @wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                raise DocumentProcessingException(
                    f"docling-serve rejected the request: {exc.response.text}"
                ) from exc
            raise UpstreamServiceException(
                service="docling-serve", message=str(exc)
            ) from exc
        except httpx.HTTPError as exc:
            raise UpstreamServiceException(
                service="docling-serve", message=str(exc)
            ) from exc

    return wrapper


def _discover_s3_result_key(
    s3_client: Minio, bucket: str, prefix: str, suffix: str
) -> str:
    """Recursively list *bucket* under *prefix* and return the sole key ending
    in *suffix*, sorted by basename for determinism (Epic 21 §21.9).

    docling-serve's S3Target writes the requested artifact under a path we
    don't fully control — a format-type subfolder always, and on some
    docling-jobkit versions an extra hash-subdirectory with no API knob to
    disable (see docs/infrastructure/docling.md). Recursive listing scoped to
    the caller's own per-obj_id prefix absorbs any of that transparently
    instead of guessing the exact key.
    """
    keys = sorted(
        (
            obj.object_name
            for obj in s3_client.list_objects(bucket, prefix=prefix, recursive=True)
            if obj.object_name and obj.object_name.endswith(suffix)
        ),
        key=lambda k: k.rsplit("/", 1)[-1],
    )
    if not keys:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                f"docling-serve reported success but produced no {suffix!r} "
                f"artifact under s3://{bucket}/{prefix}"
            ),
        )
    if len(keys) > 1:
        logger.warning(
            "s3_result_discovery_multiple_matches",
            bucket=bucket,
            prefix=prefix,
            suffix=suffix,
            keys=keys,
        )
    return keys[0]


# ---------------------------------------------------------------------------
# Request / response models for the new async parse endpoints
# ---------------------------------------------------------------------------


class SubmitResult(BaseModel):
    parsing_task_id: str
    obj_id: str
    # Which submit_endpoint branch was taken — resolve_endpoint needs this to
    # know whether to fetch the result inline (file) or discover it via S3
    # (source; forced by /v1/convert/source/batch having no inbody target).
    # The worker forwards this whole dict as resolve's request body
    # unmodified (call_parse_resolve: `json=submit_result`), so this requires
    # no worker-side changes.
    mode: Literal["file", "source"]


class ResolveRequest(BaseModel):
    parsing_task_id: str
    obj_id: str
    mode: Literal["file", "source"]


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
# Async parse endpoints — used by the new Celery parse sub-chain
# ---------------------------------------------------------------------------


@router.post("/v1/parse/submit", response_model=SubmitResult)
@_translate_docling_errors
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

    Large PDFs are fanned out into page slices and converted concurrently
    server-side by docling-serve's Ray engine (see TODOs.md Epic 21), so no
    client-side splitting is needed.

    ``file`` (inline multipart) is submitted to docling-serve as-is.
    ``source_ref`` (S3 reference) is submitted S3-direct: this service never
    reads the bytes, only verifies the referenced object exists (fail fast
    with 422 on a stale/invalid reference rather than dispatching a job that
    would fail asynchronously and opaquely), then passes the bucket/key
    straight through. Requires docling-serve's Ray-serde patch (see
    `tools/oci/images/docling`) — see `DoclingParseClient.submit_source` for
    why this needs `/v1/convert/source/batch` specifically and why the result
    comes back via S3Target rather than inline.

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
        mode = "file"
        content = await file.read()
        in_name = filename or file.filename or "upload"
        in_type = content_type or file.content_type or "application/octet-stream"
        logger.info("submit_started", obj_id=obj_id, filename=in_name, mode=mode)
        task_id = await docling_client.submit_file(
            content=content,
            filename=in_name,
            content_type=in_type,
            timeout=120.0,
        )
    else:
        mode = "source"
        ref = BlobRef.model_validate_json(source_ref)
        logger.info(
            "submit_started",
            obj_id=obj_id,
            mode=mode,
            bucket=ref.bucket,
            key=ref.key,
        )
        if not await blob_store(ref.bucket).aexists(ref.key):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"source_ref does not exist: s3://{ref.bucket}/{ref.key}",
            )
        task_id = await docling_client.submit_source(
            s3_endpoint=settings.S3_ENDPOINT,
            s3_access_key=settings.S3_ACCESS_KEY,
            s3_secret_key=settings.S3_SECRET_KEY,
            s3_secure=settings.S3_SECURE,
            bucket=ref.bucket,
            key=ref.key,
            target_bucket=settings.REPLAY_CACHE_BUCKET,
            target_key_prefix=service.convert_scratch_prefix(obj_id),
            timeout=120.0,
        )

    logger.info("submit_queued", obj_id=obj_id, parsing_task_id=task_id)
    return SubmitResult(parsing_task_id=task_id, obj_id=obj_id, mode=mode)


@router.get("/v1/parse/status/{task_id}", response_model=ParseStatus)
@_translate_docling_errors
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
@_translate_docling_errors
async def resolve_endpoint(
    request: ResolveRequest,
    docling_client: docling_client_dependency,
    blob_store: blob_store_factory_dependency,
    s3_client: s3_client_dependency,
) -> ResolveResult:
    """Fetch the completed conversion, cache it, and derive its pages.

    Must only be called after /v1/parse/status/{task_id} returns "success".
    ``request.mode`` (threaded through unchanged from /v1/parse/submit's
    response via the worker) decides how the result is retrieved: "file"
    submissions fetch it inline via GET /v1/result/{task_id}; "source"
    submissions went through /v1/convert/source/batch, whose target has no
    inbody option, so docling-serve wrote it to an S3Target scratch prefix
    instead — this discovers the exact written key via a recursive listing
    (_discover_s3_result_key, since docling-jobkit nests the artifact under
    path segments not fully under our control), reads it once, copies it to
    the stable replay_key() location, and deletes the scratch object.

    Either way, writes the lossless DoclingDocument JSON to the replay cache
    — used both for citation/re-chunk lookups and as the source
    /v1/chunk/submit reads — and derives page-level Markdown parents right
    away, while the DoclingDocument is already in hand. Chunking is a fully
    separate stage from here on (Epic 21 §21.8): this endpoint never submits
    a chunk job itself.
    """
    obj_id = request.obj_id
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)

    if request.mode == "file":
        dl_doc = await docling_client.fetch_conversion_result(
            request.parsing_task_id,
            timeout=settings.DOCLING_RESOLVE_TIMEOUT,
        )
        replay_bytes = dl_doc.model_dump_json().encode()
    else:
        scratch_prefix = service.convert_scratch_prefix(obj_id)
        scratch_key = _discover_s3_result_key(
            s3_client, settings.REPLAY_CACHE_BUCKET, scratch_prefix, ".json"
        )
        replay_bytes = await replay_store.aget(scratch_key)
        await replay_store.adelete(scratch_key)
        dl_doc = DoclingDocument.model_validate_json(replay_bytes)

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
@_translate_docling_errors
async def chunk_submit_endpoint(
    request: ChunkSubmitRequest,
    docling_client: docling_client_dependency,
    blob_store: blob_store_factory_dependency,
) -> ChunkSubmitResult:
    """Submit the replay-cached DoclingDocument for hybrid chunking.

    Must only be called after /v1/parse/resolve has written the replay cache
    for this obj_id. No chunk endpoint in docling-serve v1.29.0 accepts an S3
    source (same schema-level 422 as convert's non-batch endpoint, and there's
    no batch variant for chunk to work around it with — see submit_endpoint's
    docstring), so this service reads the replay-cached JSON itself and
    submits it inline.

    Poll GET /v1/chunk/status/{task_id} until "success", then call
    POST /v1/chunk/finalize.
    """
    obj_id = request.obj_id
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)
    replay_bytes = await replay_store.aget(service.replay_key(obj_id))
    chunking_task_id = await docling_client.submit_chunk(replay_bytes, timeout=120.0)
    logger.info("chunk_submit_queued", obj_id=obj_id, chunking_task_id=chunking_task_id)
    return ChunkSubmitResult(chunking_task_id=chunking_task_id, obj_id=obj_id)


@router.get("/v1/chunk/status/{task_id}", response_model=ParseStatus)
@_translate_docling_errors
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
@_translate_docling_errors
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
