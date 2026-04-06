"""Fixtures for data_sources unit tests.

All external I/O is replaced:
  - storage service   → httpx mock (AsyncMock)
  - KafkaConnect API  → unittest.mock.patch on KafkaConnect class
  - Postgres          → in-memory SQLite via aiosqlite
"""

from __future__ import annotations

import os
import uuid

os.environ.setdefault("SQL_DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("STORAGE_SERVICE_URL", "http://localhost:7000")
os.environ.setdefault("KAFKA_CONNECT_URL", "http://localhost:8083")

from typing import Iterator  # noqa: E402
from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import pytest  # noqa: E402
from fastapi import status  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.backend.enterprise.data_sources.api.dependencies import (  # noqa: E402
    get_db_session,
)
from src.backend.enterprise.data_sources.api.main import app  # noqa: E402
from src.backend.enterprise.data_sources.api.models import Base  # noqa: E402
from src.lib.backend.db.session import get_session  # noqa: E402


_NAMESPACE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_SOURCE_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

NAMESPACE_RESPONSE = {"id": str(_NAMESPACE_ID), "type": "shared", "name": "acme"}
_RUNNING_STATUS = {
    "connector": {"state": "RUNNING", "worker_id": "worker:8083"},
    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker:8083"}],
}


@pytest.fixture(scope="session")
async def db_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def db_factory(db_engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)


@pytest.fixture
def mock_storage_client() -> MagicMock:
    client = MagicMock()
    ns_resp = MagicMock()
    ns_resp.status_code = status.HTTP_201_CREATED
    ns_resp.json.return_value = NAMESPACE_RESPONSE

    delete_resp = MagicMock()
    delete_resp.status_code = status.HTTP_202_ACCEPTED

    client.post = AsyncMock(return_value=ns_resp)
    client.get = AsyncMock(
        return_value=MagicMock(
            status_code=status.HTTP_200_OK,
            json=lambda: NAMESPACE_RESPONSE,
        )
    )
    client.delete = AsyncMock(return_value=delete_resp)
    return client


@pytest.fixture
def mock_kafka_connect() -> MagicMock:
    kc = MagicMock()
    kc.create_connector.return_value = {}
    kc.get_connector_status.return_value = _RUNNING_STATUS
    kc.delete_connector.return_value = None
    kc.pause_connector.return_value = None
    kc.resume_connector.return_value = None
    kc.restart_connector.return_value = None
    return kc


@pytest.fixture
def client(
    mock_storage_client: MagicMock,
    mock_kafka_connect: MagicMock,
    db_factory: async_sessionmaker[AsyncSession],
) -> Iterator[TestClient]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    async def _setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    import asyncio

    asyncio.get_event_loop().run_until_complete(_setup())

    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def _get_session():
        async for session in get_session(factory):
            yield session

    import unittest.mock as mock

    with mock.patch(
        "src.backend.enterprise.data_sources.api.sources.service.KafkaConnect",
        return_value=mock_kafka_connect,
    ):
        app.dependency_overrides[get_db_session] = _get_session
        with TestClient(app) as c:
            app.state.storage_client = mock_storage_client
            yield c
        app.dependency_overrides.clear()

    engine.sync_engine.dispose()
