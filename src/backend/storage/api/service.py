"""Shared service helpers used by both namespaces and files service modules."""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.storage.api.exceptions import NamespaceNotFoundError
from src.backend.storage.api.models import Namespace


async def _fetch_namespace(session: AsyncSession, namespace_id: uuid.UUID) -> Namespace:
    result = await session.execute(
        sa.select(Namespace).where(
            Namespace.id == namespace_id,
            Namespace.deleted_at.is_(None),
        )
    )
    namespace = result.scalar_one_or_none()
    if namespace is None:
        raise NamespaceNotFoundError()
    return namespace
