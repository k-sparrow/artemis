from __future__ import annotations

import uuid

from fastapi import APIRouter, status

from src.backend.enterprise.data_sources.api.dependencies import (
    db_session_dependency,
    storage_client_dependency,
)
from src.backend.enterprise.data_sources.api.sources import service
from src.backend.enterprise.data_sources.api.sources.schemas import (
    DataSourceCreate,
    DataSourceResponse,
)
from src.lib.backend.logging import get_logger

router = APIRouter(prefix="/data-sources", tags=["data-sources"])
log = get_logger(__name__)


@router.post("", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_data_source(
    body: DataSourceCreate,
    session: db_session_dependency,
    http: storage_client_dependency,
) -> DataSourceResponse:
    result = await service.create_data_source(
        session=session,
        http=http,
        display_name=body.display_name,
        path=body.path,
        namespace=body.namespace,
        org_name=body.org_name,
    )
    log.info(
        "data_source_created",
        id=str(result.id),
        connector_name=result.connector_name,
        namespace=body.namespace,
        path=body.path,
    )
    return result


@router.get("", response_model=list[DataSourceResponse])
async def list_data_sources(session: db_session_dependency) -> list[DataSourceResponse]:
    return await service.list_data_sources(session=session)


@router.get("/{source_id}", response_model=DataSourceResponse)
async def get_data_source(
    source_id: uuid.UUID,
    session: db_session_dependency,
) -> DataSourceResponse:
    return await service.get_data_source(session=session, source_id=source_id)


@router.delete(
    "/{source_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None
)
async def delete_data_source(
    source_id: uuid.UUID,
    session: db_session_dependency,
    http: storage_client_dependency,
) -> None:
    await service.delete_data_source(
        session=session,
        http=http,
        source_id=source_id,
    )
    log.info("data_source_deleted", id=str(source_id))


@router.post("/{source_id}/pause", response_model=DataSourceResponse)
async def pause_data_source(
    source_id: uuid.UUID,
    session: db_session_dependency,
) -> DataSourceResponse:
    return await service.pause_data_source(session=session, source_id=source_id)


@router.post("/{source_id}/resume", response_model=DataSourceResponse)
async def resume_data_source(
    source_id: uuid.UUID,
    session: db_session_dependency,
) -> DataSourceResponse:
    return await service.resume_data_source(session=session, source_id=source_id)


@router.post("/{source_id}/restart", response_model=DataSourceResponse)
async def restart_data_source(
    source_id: uuid.UUID,
    session: db_session_dependency,
) -> DataSourceResponse:
    return await service.restart_data_source(session=session, source_id=source_id)
