from typing import List

from fastapi import APIRouter, File, UploadFile

from src.backend.parsing.api.dependencies import loader_factory_dependency
from src.backend.parsing.api.parse import service
from src.lib.backend.logging import get_logger
from src.lib.core.ingestion.types import ParsedChunk

__all__ = ["router"]


logger = get_logger("parsing.router")

router = APIRouter(tags=["Parsing"])


@router.post("/v1/parse", response_model=List[ParsedChunk])
async def parse_endpoint(
    loader_factory: loader_factory_dependency,
    file: UploadFile = File(...),
) -> List[ParsedChunk]:
    logger.info("parse_started", filename=file.filename, content_type=file.content_type)
    try:
        chunks = await service.a_parse(file, loader_factory)
        logger.info("parse_completed", filename=file.filename, num_chunks=len(chunks))
        return chunks
    except Exception as e:
        logger.error("parse_failed", filename=file.filename, error=str(e))
        raise
