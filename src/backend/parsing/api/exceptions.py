from fastapi import status
from fastapi.responses import JSONResponse


from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamServiceException,
)

__all__ = [
    "EXCEPTION_HANDLER_MAPPING",
]


async def document_processing_exception_handler(
    request, exc: DocumentProcessingException
):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc), "type": "document_processing_error"},
    )


async def upstream_service_exception_handler(request, exc: UpstreamServiceException):
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
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
