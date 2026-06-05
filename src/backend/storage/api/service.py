"""Shared service helpers used by both namespaces and files service modules."""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.storage.api.exceptions import (
    NamespaceAccessDeniedError,
    NamespaceNotFoundError,
)
from src.backend.storage.api.models import Namespace, NamespaceType


async def _fetch_namespace(
    session: AsyncSession,
    namespace_id: uuid.UUID,
    caller_owner_id: uuid.UUID,
    require_write: bool = False,
) -> Namespace:
    """Fetch a namespace and enforce access control.

    Access rules:
      - Write (require_write=True): caller must own the namespace regardless of type.
      - Read on PRIVATE: caller must own the namespace.
      - Read on SHARED: any authenticated caller (owner_id present) is allowed.

    Raises NamespaceNotFoundError (404) if the namespace does not exist.
    Raises NamespaceAccessDeniedError (403) on ownership mismatch.
    """
    result = await session.execute(
        sa.select(Namespace).where(
            Namespace.id == namespace_id,
            Namespace.deleted_at.is_(None),
        )
    )
    namespace = result.scalar_one_or_none()
    if namespace is None:
        raise NamespaceNotFoundError()

    if require_write or namespace.type == NamespaceType.PRIVATE:
        if namespace.owner_id != caller_owner_id:
            raise NamespaceAccessDeniedError()

    return namespace
