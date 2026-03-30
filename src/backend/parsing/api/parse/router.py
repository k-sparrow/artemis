import json
from typing import Annotated, List

from fastapi import APIRouter, File, Form, UploadFile
from pydantic import BeforeValidator

from src.backend.parsing.api.dependencies import loader_factory_dependency
from src.backend.parsing.api.parse import service
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.types import ParsedChunk

__all__ = ["router"]


logger = get_logger("parsing.router")

router = APIRouter(tags=["Parsing"])

# Form fields are always strings on the wire; this validator JSON-parses the
# raw string into dict[str, str] before Pydantic validates the type.
_JsonDict = Annotated[dict[str, str], BeforeValidator(json.loads), Form()]


@router.post("/v1/parse", response_model=List[ParsedChunk])
async def parse_endpoint(
    loader_factory: loader_factory_dependency,
    file: UploadFile = File(...),
    metadata: _JsonDict = "{}",
) -> List[ParsedChunk]:
    logger.info("parse_started", filename=file.filename, content_type=file.content_type)
    try:
        chunks = await service.a_parse(
            object=file, loader_factory=loader_factory, metadata=metadata
        )
        logger.info("parse_completed", filename=file.filename, num_chunks=len(chunks))
        return chunks
    except Exception as e:
        logger.error("parse_failed", filename=file.filename, error=str(e))
        raise
