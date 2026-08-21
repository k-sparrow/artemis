from __future__ import annotations

from fastapi import Request, status
from fastapi.responses import JSONResponse


class PathNotFoundError(Exception):
    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"File not found on mount: {path}")


class PathEscapesWatchRootError(Exception):
    def __init__(self, path: str, resolved: str) -> None:
        self.path = path
        self.resolved = resolved
        super().__init__(f"Resolved path escapes the watch root: {path} -> {resolved}")


class UrlFetchError(Exception):
    def __init__(self, url: str, upstream_status: int | None) -> None:
        self.url = url
        self.upstream_status = upstream_status
        detail = (
            f"Failed to fetch URL (HTTP {upstream_status}): {url}"
            if upstream_status
            else f"Failed to fetch URL (connection error): {url}"
        )
        super().__init__(detail)


class NamespaceNotFoundError(Exception):
    def __init__(self, namespace_id: str) -> None:
        self.namespace_id = namespace_id
        super().__init__(f"Namespace not found: {namespace_id}")


class StorageServiceError(Exception):
    def __init__(self, upstream_status: int, detail: str) -> None:
        self.upstream_status = upstream_status
        self.detail = detail
        super().__init__(detail)


def _namespace_not_found_handler(
    req: Request, exc: NamespaceNotFoundError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": f"Namespace not found: {exc.namespace_id}"},
    )


def _path_not_found_handler(req: Request, exc: PathNotFoundError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": f"File not found on mount: {exc.path}"},
    )


def _path_escapes_watch_root_handler(
    req: Request, exc: PathEscapesWatchRootError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": str(exc)},
    )


def _url_fetch_handler(req: Request, exc: UrlFetchError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={"detail": str(exc)},
    )


def _storage_service_handler(req: Request, exc: StorageServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={
            "detail": f"Storage service error ({exc.upstream_status}): {exc.detail}"
        },
    )


EXCEPTION_HANDLER_MAP = {
    NamespaceNotFoundError: _namespace_not_found_handler,
    PathNotFoundError: _path_not_found_handler,
    PathEscapesWatchRootError: _path_escapes_watch_root_handler,
    UrlFetchError: _url_fetch_handler,
    StorageServiceError: _storage_service_handler,
}
