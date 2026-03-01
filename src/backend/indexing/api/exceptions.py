from fastapi import Request
from fastapi.responses import JSONResponse

from src.lib.core.ingestion.exceptions import DocumentProcessingException

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


EXCEPTION_HANDLER_MAPPING = {
    DocumentProcessingException: document_processing_exception_handler,
}
