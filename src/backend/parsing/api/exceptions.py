from fastapi import status
from fastapi.responses import JSONResponse


from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamBackpressureException,
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


async def upstream_backpressure_exception_handler(
    request, exc: UpstreamBackpressureException
):
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={
            "detail": exc.message,
            "type": "upstream_backpressure_error",
            "service": exc.service,
        },
        headers={"Retry-After": "5"},
    )


EXCEPTION_HANDLER_MAPPING = {
    DocumentProcessingException: document_processing_exception_handler,
    # Both entries are safe regardless of dict order: starlette's lookup
    # walks type(exc).__mro__ and type(exc) is always its own first
    # element, so a raised UpstreamBackpressureException matches its own
    # handler on the first iteration, never falling through to the parent
    # UpstreamServiceException's.
    UpstreamBackpressureException: upstream_backpressure_exception_handler,
    UpstreamServiceException: upstream_service_exception_handler,
}
