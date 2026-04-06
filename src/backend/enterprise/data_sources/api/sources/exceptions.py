from __future__ import annotations

from fastapi import Request, status
from fastapi.responses import JSONResponse


class DataSourceNotFoundError(Exception):
    def __init__(self, connector_id: str) -> None:
        self.connector_id = connector_id
        super().__init__(f"Data source not found: {connector_id}")


class KafkaConnectError(Exception):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class StorageServiceError(Exception):
    def __init__(self, upstream_status: int, detail: str) -> None:
        self.upstream_status = upstream_status
        self.detail = detail
        super().__init__(detail)


def _data_source_not_found_handler(
    req: Request, exc: DataSourceNotFoundError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": f"Data source not found: {exc.connector_id}"},
    )


def _kafka_connect_error_handler(req: Request, exc: KafkaConnectError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={"detail": f"Kafka Connect error: {exc.detail}"},
    )


def _storage_service_error_handler(
    req: Request, exc: StorageServiceError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={
            "detail": f"Storage service error ({exc.upstream_status}): {exc.detail}"
        },
    )


EXCEPTION_HANDLER_MAP = {
    DataSourceNotFoundError: _data_source_not_found_handler,
    KafkaConnectError: _kafka_connect_error_handler,
    StorageServiceError: _storage_service_error_handler,
}
