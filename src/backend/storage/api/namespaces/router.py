"""FastAPI router for /namespaces — namespace lifecycle only.

Owner identity
--------------
In stub mode the owner UUID is read from the ``X-Owner-Id`` request header.
In enterprise mode this header is injected by the API gateway after JWT
verification — the storage service never validates tokens directly.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Header, status

from src.backend.storage.api.dependencies import db_session_dependency
from src.backend.storage.api.namespaces import service
from src.backend.storage.api.namespaces.schemas import (
    NamespaceCreate,
    NamespaceRename,
    NamespaceResponse,
)
from src.lib.backend.logging import get_logger

router = APIRouter(tags=["namespaces"])
log = get_logger(__name__)


@router.post("", response_model=NamespaceResponse, status_code=status.HTTP_201_CREATED)
async def create_namespace_endpoint(
    body: NamespaceCreate,
    session: db_session_dependency,
    owner_id: str = Header(..., alias="X-Owner-Id"),
) -> NamespaceResponse:
    owner_uuid = service.parse_owner_id(owner_id)
    namespace = await service.create_namespace(
        session=session,
        owner_id=owner_uuid,
        type_=body.type,
        name=body.name,
    )
    log.info(
        "namespace_created", id=namespace.id, type=namespace.type, owner=owner_uuid
    )
    return NamespaceResponse.model_validate(namespace)


@router.get("", response_model=list[NamespaceResponse])
async def list_namespaces_endpoint(
    session: db_session_dependency,
    owner_id: str = Header(..., alias="X-Owner-Id"),
) -> list[NamespaceResponse]:
    owner_uuid = service.parse_owner_id(owner_id)
    namespaces = await service.list_namespaces(session=session, owner_id=owner_uuid)
    return [NamespaceResponse.model_validate(n) for n in namespaces]


@router.get("/{namespace_id}", response_model=NamespaceResponse)
async def get_namespace_endpoint(
    namespace_id: uuid.UUID,
    session: db_session_dependency,
) -> NamespaceResponse:
    namespace = await service.get_namespace(session=session, namespace_id=namespace_id)
    return NamespaceResponse.model_validate(namespace)


@router.patch("/{namespace_id}", response_model=NamespaceResponse)
async def rename_namespace_endpoint(
    namespace_id: uuid.UUID,
    body: NamespaceRename,
    session: db_session_dependency,
) -> NamespaceResponse:
    namespace = await service.rename_namespace(
        session=session, namespace_id=namespace_id, new_name=body.name
    )
    return NamespaceResponse.model_validate(namespace)


@router.delete("/{namespace_id}", status_code=status.HTTP_202_ACCEPTED)
async def soft_delete_namespace_endpoint(
    namespace_id: uuid.UUID,
    session: db_session_dependency,
) -> None:
    await service.soft_delete_namespace(session=session, namespace_id=namespace_id)
    log.info("namespace_soft_deleted", id=namespace_id)
    # TODO(epic-1.2): dispatch delete_namespace Celery task via apply_async()
