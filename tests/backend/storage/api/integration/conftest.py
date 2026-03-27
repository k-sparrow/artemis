"""Tier 2 integration test fixtures for the storage service.

Infrastructure (session-scoped):
  - PostgresContainer  — real Postgres; storage schema created via create_all
  - MinioContainer     — real MinIO; artemis bucket created on start

Per-test cleanup (autouse):
  - clean_db    — TRUNCATE storage tables after each test
  - clean_minio — remove all objects from the artemis bucket after each test

Event-loop isolation
--------------------
TestClient uses anyio internally and runs each request in its own event loop.
asyncpg connections are bound to the loop that created them.  If the client
fixture shared the session-scoped engine, anyio-loop connections would leak
back into the pool and cause "Future attached to a different loop" errors when
clean_db teardown runs on pytest-asyncio's session loop.

Fix: the client fixture creates its own engine per test and disposes it
(sync) before returning control to clean_db, so no anyio-loop connections
survive in the shared pool.

The FastAPI lifespan is still mocked (parent conftest autouse) because it
calls link_s3_bucket_with_kafka_event which requires a Kafka ARN registered
in MinIO — not available in plain testcontainers.  Schema creation and bucket
setup are handled here instead.
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncIterator, Iterator

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from src.backend.storage.api.dependencies import get_db_session, get_minio_client
from src.backend.storage.api.main import app
from src.backend.storage.api.models import Base
from src.lib.backend.db.session import get_session


# ---------------------------------------------------------------------------
# Infrastructure containers (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container(request: pytest.FixtureRequest) -> PostgresContainer:
    container = PostgresContainer(
        image="postgres:16-alpine",
        username="storage",
        password="storage",
        dbname="storage_test",
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def minio_container(request: pytest.FixtureRequest) -> MinioContainer:
    container = MinioContainer()
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# Derived fixtures (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def async_db_url(postgres_container: PostgresContainer) -> str:
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    u = postgres_container.username
    p = postgres_container.password
    db = postgres_container.dbname
    return f"postgresql+asyncpg://{u}:{p}@{host}:{port}/{db}"


@pytest.fixture(scope="session")
async def storage_session_factory(
    async_db_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Session-scoped engine used by clean_db and seed helpers (session loop only).

    Yields (not returns) so the engine stays alive for the full test session.
    The engine pool is used exclusively from pytest-asyncio's session loop —
    never from TestClient's anyio loop (see client fixture below).
    """
    engine = create_async_engine(async_db_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    yield factory
    await engine.dispose()


@pytest.fixture(scope="session")
def test_minio_client(minio_container: MinioContainer) -> Minio:
    client = minio_container.get_client()
    bucket = os.environ["S3_ARTEMIS_BUCKET"]
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    return client


# ---------------------------------------------------------------------------
# TestClient (function-scoped) — real DB + MinIO, mocked lifespan
# ---------------------------------------------------------------------------


@pytest.fixture
def client(
    async_db_url: str,
    test_minio_client: Minio,
    storage_session_factory: async_sessionmaker[AsyncSession],  # ensures schema exists
) -> Iterator[TestClient]:
    """Each test gets its own engine so TestClient's anyio-loop connections
    are fully isolated from the session-loop pool used by clean_db."""
    engine = create_async_engine(async_db_url)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def _get_session():
        async for session in get_session(factory):
            yield session

    app.dependency_overrides[get_db_session] = _get_session
    app.dependency_overrides[get_minio_client] = lambda: test_minio_client
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    # Synchronous dispose: closes all anyio-loop connections before clean_db runs.
    engine.sync_engine.dispose()


# ---------------------------------------------------------------------------
# Per-test cleanup (autouse)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
async def clean_db(
    storage_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    yield
    async with storage_session_factory() as session:
        await session.execute(
            sa.text("TRUNCATE owner, namespace, ingested_file CASCADE")
        )
        await session.commit()


@pytest.fixture(autouse=True)
def clean_minio(test_minio_client: Minio) -> Iterator[None]:
    yield
    bucket = os.environ["S3_ARTEMIS_BUCKET"]
    for obj in test_minio_client.list_objects(bucket, recursive=True):
        test_minio_client.remove_object(bucket, obj.object_name)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def owner_id() -> str:
    return str(uuid.uuid4())
