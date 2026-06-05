from fastapi import Request, responses, status


class NamespaceNotFoundError(Exception):
    pass


class NamespaceAccessDeniedError(Exception):
    pass


def _namespace_not_found_handler(request: Request, exc: NamespaceNotFoundError):
    return responses.JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": "Namespace not found"},
    )


def _namespace_access_denied_handler(request: Request, exc: NamespaceAccessDeniedError):
    return responses.JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={"detail": "Access denied"},
    )


EXCEPTION_HANDLER_MAP = {
    NamespaceNotFoundError: _namespace_not_found_handler,
    NamespaceAccessDeniedError: _namespace_access_denied_handler,
}
