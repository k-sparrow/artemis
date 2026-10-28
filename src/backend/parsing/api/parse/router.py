import asyncio
import json
import uuid
from functools import wraps
from typing import Annotated, Literal, Optional

import httpx
from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import DoclingDocument
from docling.datamodel.service.callbacks import (
    ProgressCallbackRequest,
    ProgressCallbackResponse,
    ProgressKind,
)
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from minio import Minio
from minio.commonconfig import CopySource
from pydantic import BaseModel, BeforeValidator

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    blob_store_factory_dependency,
    celery_producer_dependency,
    docling_client_dependency,
    s3_client_dependency,
)
from src.backend.parsing.api.parse import service
from src.backend.parsing.lib.artifact import (
    ParseStatus,
    build_pages,
    build_pages_from_markdown,
)
from src.lib.backend.logging import get_logger
from src.lib.core.adapters.stores.base.blob import BlobStore
from src.lib.core.ingestion.contract import BlobRef
from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamBackpressureException,
    UpstreamServiceException,
)

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
    retries. A docling-serve 429 (queue-full backpressure — see
    ``DOCLING_SERVE_ENG_RAY_ENABLE_QUEUE_LIMIT_REJECTION``) means the request
    is fine but the queue is full right now, not that it's wrong — mapped to
    its own 429 (``UpstreamBackpressureException``) so the worker's dedicated
    429 handling engages (flat-delay retry, 24h budget, excluded from the
    circuit breaker's failure count — see submit_parse/submit_chunk in
    controller/worker/tasks.py). Collapsing it into the generic "4xx = this
    request was rejected" branch below silently defeated that entire
    backpressure design: the worker never saw a 429 at all, just an
    indistinguishable-from-permanent 400, which also legitimately tripped the
    circuit breaker after 3 consecutive occurrences under real load
    (confirmed live against a ~900-document burst). A genuine 4xx (e.g. a
    malformed submission) still maps to 400 (``DocumentProcessingException``)
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
            if exc.response.status_code == 429:
                raise UpstreamBackpressureException(
                    service="docling-serve",
                    message=f"queue full: {exc.response.text}",
                ) from exc
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


async def _run_cpu(fn, *args):
    """Run CPU-bound *fn* off the event loop.

    The one place that decides where such work runs. A thread today, which
    frees the loop during Python-level work but not during a single long C
    call that holds the GIL (json.loads, pydantic-core). A process pool here
    needs plain data in and out: service.build_artifact already is; /resolve
    still passes DoclingDocument across.
    """
    return await asyncio.to_thread(fn, *args)


def _list_s3_result_keys(
    s3_client: Minio, bucket: str, prefix: str, suffix: str
) -> list[str]:
    """Keys under *prefix* ending in *suffix*, sorted by basename."""
    return sorted(
        (
            obj.object_name
            for obj in s3_client.list_objects(bucket, prefix=prefix, recursive=True)
            if obj.object_name and obj.object_name.endswith(suffix)
        ),
        key=lambda k: k.rsplit("/", 1)[-1],
    )


# Leading bytes of the converted JSON read to recover origin.filename (see
# service.origin_filename_from_head) — far more than the handful of fields
# that precede it need.
_ORIGIN_HEAD_BYTES = 64 * 1024


def _read_origin_filename(s3_client: Minio, bucket: str, key: str) -> str:
    response = s3_client.get_object(bucket, key, offset=0, length=_ORIGIN_HEAD_BYTES)
    try:
        head = response.read()
    finally:
        response.close()
        response.release_conn()
    return service.origin_filename_from_head(head)


def _pages_from_markdown(
    markdown_bytes: bytes, obj_id: uuid.UUID, source: str
) -> tuple[bytes, int]:
    pages = build_pages_from_markdown(markdown_bytes.decode("utf-8"), obj_id, source)
    return service.encode_pages(pages), len(pages)


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
    keys = _list_s3_result_keys(s3_client, bucket, prefix, suffix)
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


async def _marker_matches(store: BlobStore, key: str, task_id: str) -> bool:
    """Whether the retry-safety marker at *key* names *task_id* (see
    service.resolved_marker_key)."""
    if not await store.aexists(key):
        return False
    return await store.aget(key) == task_id.encode()


def _dump_replay(dl_doc: DoclingDocument) -> bytes:
    return dl_doc.model_dump_json().encode()


def _derive_pages(dl_doc: DoclingDocument, obj_id: uuid.UUID) -> tuple[bytes, int]:
    pages = build_pages(dl_doc, obj_id)
    return service.encode_pages(pages), len(pages)


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
    callback_url = (
        f"{settings.PARSING_SERVICE_PUBLIC_URL.rstrip('/')}/v1/parse/callback/{obj_id}"
    )
    callbacks = [{"url": callback_url}]

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
            callbacks=callbacks,
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
            callbacks=callbacks,
            timeout=120.0,
        )

    logger.info("submit_queued", obj_id=obj_id, parsing_task_id=task_id)
    return SubmitResult(parsing_task_id=task_id, obj_id=obj_id, mode=mode)


@router.post("/v1/parse/callback/{obj_id}", response_model=ProgressCallbackResponse)
async def parse_callback_endpoint(
    obj_id: str,
    callback: ProgressCallbackRequest,
    producer: celery_producer_dependency,
) -> ProgressCallbackResponse:
    """Receive docling-serve's terminal conversion callback; hand it to Celery.

    Registered as the callback URL on every /v1/parse/submit call (see
    submit_endpoint). Delivery is fire-once, best-effort — docling-serve
    retries this call 3x over ~1-2s then silently drops it — so poll_parse
    keeps polling as a safety net regardless of whether this ever fires.
    Races poll_parse for the same document; ``tasks.advance_from_callback``
    is what actually arbitrates that race (see backend/claims.py), not
    anything here.

    The HTTP status here reflects ONLY whether the payload was successfully
    handed off to Celery — never the document's conversion outcome. A 200 is
    returned as soon as ``send_task`` succeeds, regardless of whether the
    document itself converted successfully or failed; that outcome is
    ``advance_from_callback``'s problem to resolve asynchronously. Only a
    ``send_task`` failure (broker unreachable) returns non-2xx — the one case
    docling-serve's ``CallbackInvoker`` does something useful with (it
    retries redelivering the callback).
    """
    progress = callback.progress
    if progress.kind != ProgressKind.DOCUMENT_COMPLETED:
        # set_num_docs / update_processed — nothing to advance yet, ack only.
        return ProgressCallbackResponse()

    doc_status = progress.document.status
    if doc_status == ConversionStatus.SUCCESS:
        outcome = "success"
        error_message = None
    elif doc_status == ConversionStatus.PARTIAL_SUCCESS:
        # Mirrors DoclingParseClient.get_status: some page slices failed
        # server-side (Ray engine fan-out) — treat as failure so the retry
        # re-submits the whole document rather than proceeding with a
        # truncated DoclingDocument.
        outcome = "failure"
        error_message = (
            f"partial_success: {progress.document.error}"
            if progress.document.error
            else "partial_success"
        )
    else:
        outcome = "failure"
        error_message = progress.document.error or f"status={doc_status.value}"

    try:
        # A blocking broker publish: off the event loop, and without Celery's
        # own publish retries — docling-serve redelivers this callback and
        # poll_parse is the safety net, so a quick 503 beats holding a thread
        # on an unreachable broker.
        await asyncio.to_thread(
            producer.send_task,
            "tasks.advance_from_callback",
            args=[obj_id],
            kwargs={
                "stage": "convert",
                "docling_task_id": callback.task_id,
                "outcome": outcome,
                "error_message": error_message,
            },
            queue="artemis.ingestion.parse",
            exchange=settings.EXCHANGE_NAME,
            routing_key="parse",
            retry=False,
        )
    except Exception as exc:
        logger.warning("parse_callback_enqueue_failed", obj_id=obj_id, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="failed to enqueue advance_from_callback",
        ) from exc

    logger.info(
        "parse_callback_received",
        obj_id=obj_id,
        docling_task_id=callback.task_id,
        outcome=outcome,
    )
    return ProgressCallbackResponse()


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
    instead — this discovers the exact written keys via a recursive listing
    (_discover_s3_result_key, since docling-jobkit nests the artifact under
    path segments not fully under our control).

    Either way the lossless DoclingDocument JSON ends up in the replay cache
    — used both for citation/re-chunk lookups and as the source
    /v1/chunk/submit reads — along with the page-level Markdown parents.
    Chunking is a fully separate stage from here on (Epic 21 §21.8): this
    endpoint never submits a chunk job itself.

    In "source" mode the document itself never passes through this service
    (the claim-check rule, applied to resolve): the JSON is moved to
    replay_key() by an S3 server-side copy, and the pages come from the
    page-delimited Markdown docling-serve wrote next to it (see
    DoclingParseClient.submit_source). That keeps this call short however
    large the document is. A conversion submitted before Markdown was
    requested has no such file; it falls back to loading the JSON, like
    "file" mode.

    Safe to retry: for a very large document this call can outlive the
    worker's HTTP timeout and still complete here, so the worker's retry must
    not fail on the now-consumed result. A marker naming the resolved
    ``parsing_task_id`` is written last, and a repeat call for that same task
    returns immediately. Scratch objects are only deleted after the marker,
    so an attempt that dies part-way leaves everything a retry needs.

    All (de)serialisation of the DoclingDocument runs in a worker thread —
    at this size it's long enough to stall every other request on the event
    loop, status polls for unrelated documents included.
    """
    obj_id = request.obj_id
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)
    marker_key = service.resolved_marker_key(obj_id)

    if await _marker_matches(replay_store, marker_key, request.parsing_task_id):
        logger.info(
            "resolve_already_done",
            obj_id=obj_id,
            parsing_task_id=request.parsing_task_id,
        )
        return ResolveResult(obj_id=obj_id)

    bucket = settings.REPLAY_CACHE_BUCKET
    scratch_keys: list[str] = []
    markdown_key: str | None = None
    if request.mode == "file":
        dl_doc = await docling_client.fetch_conversion_result(
            request.parsing_task_id,
            timeout=settings.DOCLING_RESOLVE_TIMEOUT,
        )
        replay_bytes = await _run_cpu(_dump_replay, dl_doc)
    else:
        scratch_prefix = service.convert_scratch_prefix(obj_id)
        json_key = await asyncio.to_thread(
            _discover_s3_result_key, s3_client, bucket, scratch_prefix, ".json"
        )
        markdown_keys = await asyncio.to_thread(
            _list_s3_result_keys, s3_client, bucket, scratch_prefix, ".md"
        )
        scratch_keys = [json_key, *markdown_keys]
        if markdown_keys:
            markdown_key = markdown_keys[0]
        else:
            replay_bytes = await replay_store.aget(json_key)
            dl_doc = await _run_cpu(DoclingDocument.model_validate_json, replay_bytes)

    if markdown_key is not None:
        await asyncio.to_thread(
            s3_client.copy_object,
            bucket,
            service.replay_key(obj_id),
            CopySource(bucket, json_key),
        )
        logger.info("replay_cache_written", obj_id=obj_id, via="s3_copy")
        source = await asyncio.to_thread(
            _read_origin_filename, s3_client, bucket, json_key
        )
        pages_bytes, num_pages = await _run_cpu(
            _pages_from_markdown,
            await replay_store.aget(markdown_key),
            uuid.UUID(obj_id),
            source,
        )
    else:
        await replay_store.aput(
            service.replay_key(obj_id), replay_bytes, content_type="application/json"
        )
        logger.info("replay_cache_written", obj_id=obj_id, via="upload")
        pages_bytes, num_pages = await _run_cpu(
            _derive_pages, dl_doc, uuid.UUID(obj_id)
        )

    await replay_store.aput(
        service.pages_key(obj_id), pages_bytes, content_type="application/json"
    )
    logger.info("pages_cached", obj_id=obj_id, num_pages=num_pages)

    await replay_store.aput(marker_key, request.parsing_task_id.encode())
    for scratch_key in scratch_keys:
        await replay_store.adelete(scratch_key)

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
    chunking_task_id = await docling_client.submit_chunk(
        replay_bytes, timeout=settings.DOCLING_CHUNK_SUBMIT_TIMEOUT
    )
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

    If docling-serve's hybrid chunker produces zero chunks (a real production
    occurrence — the chunking step can degrade to nothing for a specific
    document even though conversion succeeded), fall back to a plain
    character-count split of the cached pages rather than writing an artifact
    with an empty chunk list. This lives here, not in the indexing service,
    per Epic 21.8: chunking-strategy decisions belong entirely to this
    service now. If the fallback split *also* produces nothing (pages are
    whitespace-only), that's a genuine failure — raised here rather than
    silently persisted for indexing to discover later.

    Safe to retry, same scheme as /v1/parse/resolve: a marker naming the
    finalized ``chunking_task_id`` is written after the artifact, and a
    repeat call for that task returns the existing artifact's BlobRef rather
    than re-fetching a chunk result docling-serve may have evicted by then.
    """
    obj_id_str = request.obj_id
    obj_id = uuid.UUID(obj_id_str)
    replay_store = blob_store(settings.REPLAY_CACHE_BUCKET)
    artifact_store = blob_store(settings.PARSED_ARTIFACTS_BUCKET)
    key = service.artifact_key(obj_id_str)
    marker_key = service.finalized_marker_key(obj_id_str)

    if await _marker_matches(
        replay_store, marker_key, request.chunking_task_id
    ) and await artifact_store.aexists(key):
        logger.info(
            "chunk_finalize_already_done",
            obj_id=obj_id_str,
            chunking_task_id=request.chunking_task_id,
        )
        return BlobRef(bucket=settings.PARSED_ARTIFACTS_BUCKET, key=key)

    chunk_result = await docling_client.fetch_chunk_result(
        request.chunking_task_id,
        timeout=settings.DOCLING_FINALIZE_TIMEOUT,
    )
    pages_bytes = await replay_store.aget(service.pages_key(obj_id_str))
    built = await _run_cpu(service.build_artifact, chunk_result, pages_bytes, obj_id)
    if built.fallback_used:
        logger.info(
            "chunk_finalize_fallback_used",
            obj_id=obj_id_str,
            num_fallback_chunks=built.num_chunks,
        )

    await artifact_store.aput(key, built.data, content_type="application/json")
    await replay_store.aput(marker_key, request.chunking_task_id.encode())
    logger.info(
        "chunk_finalize_completed",
        obj_id=obj_id_str,
        num_pages=built.num_pages,
        num_chunks=built.num_chunks,
        key=key,
    )
    return BlobRef(bucket=settings.PARSED_ARTIFACTS_BUCKET, key=key)
