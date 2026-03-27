from fastapi import Request, responses, status


class NamespaceNotFoundError(Exception):
    pass


def _namespace_not_found_handler(request: Request, exc: NamespaceNotFoundError):
    return responses.JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": "Namespace not found"},
    )


EXCEPTION_HANDLER_MAP = {
    NamespaceNotFoundError: _namespace_not_found_handler,
}
