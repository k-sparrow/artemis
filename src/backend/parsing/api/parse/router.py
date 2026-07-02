import json
from typing import Annotated, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BeforeValidator

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    blob_store_factory_dependency,
    loader_factory_dependency,
)
from src.backend.parsing.api.parse import service
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.contract import BlobRef

__all__ = ["router"]


logger = get_logger("parsing.router")

router = APIRouter(tags=["Parsing"])

# Form fields are always strings on the wire; this validator JSON-parses the
# raw string into dict[str, str] before Pydantic validates the type.
_JsonDict = Annotated[dict[str, str], BeforeValidator(json.loads), Form()]


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
