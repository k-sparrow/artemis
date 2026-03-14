from fastapi import Request
from fastapi.responses import JSONResponse

from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamServiceException,
)

__all__ = [
    "EXCEPTION_HANDLER_MAPPING",
]


async def document_processing_exception_handler(
    request: Request, exc: DocumentProcessingException
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.message,
            "type": "document_processing_error",
        },
    )


async def upstream_service_exception_handler(
    request: Request, exc: UpstreamServiceException
) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "detail": exc.message,
            "type": "upstream_service_error",
            "service": exc.service,
        },
    )


EXCEPTION_HANDLER_MAPPING = {
    DocumentProcessingException: document_processing_exception_handler,
    UpstreamServiceException: upstream_service_exception_handler,
}
