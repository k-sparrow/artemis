"""Business logic for the /data-sources control plane endpoints.

Flow for connector creation
---------------------------
1. Derive owner_id = uuid5(ARTEMIS_NS, org_name).
2. Allocate record_id = uuid4(); connector_name = artemis-{record_id}.
3. Upsert SHARED namespace on storage service (handles 409 idempotently).
4. Render Kafka Connect config from template.
5. Deploy connector to Kafka Connect (sync call via asyncio.to_thread).
6. Persist DataSource row to Postgres.
7. Return response enriched with live connector status.

KafkaConnect client
-------------------
kafka-connect-py uses the ``requests`` library (synchronous). It is stateless
so a new instance is created per call inside ``asyncio.to_thread`` — no
connection pool or app.state entry needed.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
import sqlalchemy as sa
from fastapi import status as http_status
from kafka_connect import KafkaConnect
from requests.exceptions import HTTPError
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.enterprise.data_sources.api.config import settings
from src.backend.enterprise.data_sources.api.models import ARTEMIS_NS, DataSource
from src.backend.enterprise.data_sources.api.sources.exceptions import (
    DataSourceNotFoundError,
    KafkaConnectError,
    StorageServiceError,
)
from src.backend.enterprise.data_sources.api.sources.schemas import (
    ConnectorTaskStatus,
    DataSourceResponse,
    KafkaConnectStatus,
)
from src.backend.enterprise.data_sources.api.sources.templates import (
    render_filesystem_connector,
)

_SOURCE_TYPE_FILESYSTEM = "filesystem"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_kafka_client():
    return KafkaConnect(url=settings.KAFKA_CONNECT_URL)


def _connector_name(record_id: uuid.UUID) -> str:
    return f"artemis-{record_id}"


def _parse_kafka_status(raw: dict[str, Any]) -> KafkaConnectStatus:
    connector_block = raw.get("connector", {})
    tasks = [
        ConnectorTaskStatus(
            id=t["id"],
            state=t["state"],
            worker_id=t["worker_id"],
            trace=t.get("trace"),
        )
        for t in raw.get("tasks", [])
    ]
    return KafkaConnectStatus(
        state=connector_block.get("state", "UNKNOWN"),
        worker_id=connector_block.get("worker_id", ""),
        tasks=tasks,
    )


async def _get_kafka_status(connector_name: str) -> KafkaConnectStatus | None:
    """Fetch live connector status. Returns None on any error."""
    kc = _make_kafka_client()
    try:
        raw = await asyncio.to_thread(kc.get_connector_status, connector_name)
        return _parse_kafka_status(raw)
    except Exception:
        return None


async def _upsert_namespace(
    http: httpx.AsyncClient,
    namespace: str,
    owner_id: uuid.UUID,
) -> uuid.UUID:
    """Upsert a SHARED namespace on the storage service.

    Returns the namespace_id. On 409 the existing UUID is read from the
    response body — the storage service is the sole authority on namespace IDs.
    """
    resp = await http.post(
        "/namespaces",
        json={"type": "shared", "name": namespace},
        headers={"X-Owner-Id": str(owner_id)},
    )

    if resp.status_code == http_status.HTTP_201_CREATED:
        return uuid.UUID(resp.json()["id"])

    if resp.status_code == http_status.HTTP_409_CONFLICT:
        # Namespace already exists — storage service returns it in the 409 body.
        return uuid.UUID(resp.json()["id"])

    raise StorageServiceError(resp.status_code, resp.text)


async def _soft_delete_namespace(
    http: httpx.AsyncClient,
    namespace_id: uuid.UUID,
) -> None:
    resp = await http.delete(f"/namespaces/{namespace_id}")
    if resp.status_code not in (
        http_status.HTTP_202_ACCEPTED,
        http_status.HTTP_404_NOT_FOUND,
    ):
        raise StorageServiceError(resp.status_code, resp.text)


async def _fetch_row(session: AsyncSession, source_id: uuid.UUID) -> DataSource:
    result = await session.execute(
        sa.select(DataSource).where(
            DataSource.id == source_id,
            DataSource.deleted_at.is_(None),
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise DataSourceNotFoundError(str(source_id))
    return row


def _to_response(
    row: DataSource, kafka_status: KafkaConnectStatus | None
) -> DataSourceResponse:
    return DataSourceResponse(
        id=row.id,
        display_name=row.name,
        source_type=row.source_type,
        connector_name=row.connector_name,
        namespace_id=row.namespace_id,
        org_name=row.org_name,
        config=row.config,
        created_at=row.created_at,
        updated_at=row.updated_at,
        kafka_status=kafka_status,
    )


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


async def create_data_source(
    session: AsyncSession,
    http: httpx.AsyncClient,
    display_name: str,
    path: str,
    namespace: str,
    org_name: str,
    recursive: bool = True,
) -> DataSourceResponse:
    owner_id = uuid.uuid5(ARTEMIS_NS, org_name)
    record_id = uuid.uuid4()
    connector_name = _connector_name(record_id)

    namespace_id = await _upsert_namespace(http, namespace, owner_id)

    config_dict = render_filesystem_connector(
        connector_name=connector_name,
        watch_path=path,
        namespace=namespace,
        namespace_id=str(namespace_id),
        org_name=org_name,
        recursive=recursive,
    )

    kc = _make_kafka_client()
    try:
        await asyncio.to_thread(kc.create_connector, config_dict)
    except HTTPError as exc:
        raise KafkaConnectError(str(exc)) from exc

    row = DataSource(
        id=record_id,
        name=display_name,
        source_type=_SOURCE_TYPE_FILESYSTEM,
        connector_name=connector_name,
        namespace_id=namespace_id,
        org_name=org_name,
        config={"path": path},
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)

    kafka_status = await _get_kafka_status(connector_name)
    return _to_response(row, kafka_status)


async def list_data_sources(session: AsyncSession) -> list[DataSourceResponse]:
    result = await session.execute(
        sa.select(DataSource).where(DataSource.deleted_at.is_(None))
    )
    rows = list(result.scalars().all())

    statuses = await asyncio.gather(
        *[_get_kafka_status(row.connector_name) for row in rows],
        return_exceptions=True,
    )

    return [
        _to_response(row, s if isinstance(s, KafkaConnectStatus) else None)
        for row, s in zip(rows, statuses)
    ]


async def get_data_source(
    session: AsyncSession,
    source_id: uuid.UUID,
) -> DataSourceResponse:
    row = await _fetch_row(session, source_id)
    kafka_status = await _get_kafka_status(row.connector_name)
    return _to_response(row, kafka_status)


async def delete_data_source(
    session: AsyncSession,
    http: httpx.AsyncClient,
    source_id: uuid.UUID,
) -> None:
    row = await _fetch_row(session, source_id)

    # Only soft-delete the namespace when no other active data source shares it.
    sibling_result = await session.execute(
        sa.select(sa.func.count()).where(
            DataSource.namespace_id == row.namespace_id,
            DataSource.deleted_at.is_(None),
            DataSource.id != source_id,
        )
    )
    has_siblings = sibling_result.scalar_one() > 0

    kc = _make_kafka_client()
    try:
        await asyncio.to_thread(kc.delete_connector, row.connector_name)
    except HTTPError as exc:
        if "404" not in str(exc):
            raise KafkaConnectError(str(exc)) from exc

    if not has_siblings:
        await _soft_delete_namespace(http, row.namespace_id)

    row.deleted_at = datetime.now(timezone.utc)
    await session.commit()


async def pause_data_source(
    session: AsyncSession,
    source_id: uuid.UUID,
) -> DataSourceResponse:
    row = await _fetch_row(session, source_id)
    kc = _make_kafka_client()
    try:
        await asyncio.to_thread(kc.pause_connector, row.connector_name)
    except HTTPError as exc:
        raise KafkaConnectError(str(exc)) from exc
    kafka_status = await _get_kafka_status(row.connector_name)
    return _to_response(row, kafka_status)


async def resume_data_source(
    session: AsyncSession,
    source_id: uuid.UUID,
) -> DataSourceResponse:
    row = await _fetch_row(session, source_id)
    kc = _make_kafka_client()
    try:
        await asyncio.to_thread(kc.resume_connector, row.connector_name)
    except HTTPError as exc:
        raise KafkaConnectError(str(exc)) from exc
    kafka_status = await _get_kafka_status(row.connector_name)
    return _to_response(row, kafka_status)


async def restart_data_source(
    session: AsyncSession,
    source_id: uuid.UUID,
) -> DataSourceResponse:
    row = await _fetch_row(session, source_id)
    kc = _make_kafka_client()
    try:
        await asyncio.to_thread(
            kc.restart_connector, row.connector_name, include_tasks=True
        )
    except HTTPError as exc:
        raise KafkaConnectError(str(exc)) from exc
    kafka_status = await _get_kafka_status(row.connector_name)
    return _to_response(row, kafka_status)
