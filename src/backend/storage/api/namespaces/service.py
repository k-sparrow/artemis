"""Namespace CRUD service functions."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.storage.api.namespaces.exceptions import (
    InvalidOwnerIdError,
    NamespaceAlreadyExistsError,
    OnlyPrivateNamespaceCanBeRenamedError,
)
from src.backend.storage.api.models import (
    ARTEMIS_NS,
    Namespace,
    NamespaceType,
    Owner,
)
from src.backend.storage.api.service import _fetch_namespace


def parse_owner_id(owner_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(owner_id)
    except ValueError:
        raise InvalidOwnerIdError()


async def create_namespace(
    session: AsyncSession,
    owner_id: uuid.UUID,
    type_: NamespaceType,
    name: str | None,
) -> Namespace:
    owner = await session.get(Owner, owner_id)
    if owner is None:
        owner = Owner(id=owner_id)
        session.add(owner)

    if type_ == NamespaceType.PRIVATE:
        ns_id = uuid.uuid4()
        ns_name = name or str(ns_id)
    else:
        ns_id = uuid.uuid5(ARTEMIS_NS, name)
        ns_name = name
        existing = await session.get(Namespace, ns_id)
        if existing is not None:
            raise NamespaceAlreadyExistsError(
                {
                    "id": str(existing.id),
                    "name": existing.name,
                    "type": existing.type.value,
                    "owner_id": str(existing.owner_id),
                    "created_at": existing.created_at.isoformat(),
                    "updated_at": existing.updated_at.isoformat(),
                }
            )

    namespace = Namespace(id=ns_id, name=ns_name, type=type_, owner_id=owner_id)
    session.add(namespace)
    await session.commit()
    await session.refresh(namespace)
    return namespace


async def get_namespace(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
) -> Namespace:
    return await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
    )


async def get_namespace_by_name(
    session: AsyncSession,
    name: str,
    caller_owner_id: uuid.UUID,
) -> Namespace:
    """Look up a SHARED namespace by name, computing UUID5 server-side."""
    ns_id = uuid.uuid5(ARTEMIS_NS, name)
    return await _fetch_namespace(
        session=session,
        namespace_id=ns_id,
        caller_owner_id=caller_owner_id,
    )


async def list_namespaces(
    session: AsyncSession,
    owner_id: uuid.UUID,
) -> list[Namespace]:
    result = await session.execute(
        sa.select(Namespace).where(
            sa.or_(
                Namespace.owner_id == owner_id,
                Namespace.type == NamespaceType.SHARED,
            ),
            Namespace.deleted_at.is_(None),
        )
    )
    return list(result.scalars().all())


async def rename_namespace(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    new_name: str,
) -> Namespace:
    namespace = await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        require_write=True,
    )
    if namespace.type != NamespaceType.PRIVATE:
        raise OnlyPrivateNamespaceCanBeRenamedError()
    namespace.name = new_name
    namespace.updated_at = datetime.now(timezone.utc)
    await session.commit()
    await session.refresh(namespace)
    return namespace


async def soft_delete_namespace(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
) -> None:
    namespace = await _fetch_namespace(
        session=session,
        namespace_id=namespace_id,
        caller_owner_id=caller_owner_id,
        require_write=True,
    )
    namespace.deleted_at = datetime.now(timezone.utc)
    await session.commit()
