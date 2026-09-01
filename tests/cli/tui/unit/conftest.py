"""Shared fixtures and data factories for TUI Pilot unit tests."""

from __future__ import annotations

import datetime
import os
import uuid
from unittest.mock import AsyncMock

import pytest

# Must be set before importing any enterprise service module whose __init__.py
# pulls in the router → config → Settings (which requires SQL_DB_URL).
os.environ.setdefault("SQL_DB_URL", "postgresql+asyncpg://test:test@localhost/test")

from src.backend.enterprise.data_sources.api.sources.schemas import (  # noqa: E402
    DataSourceResponse,
)
from src.backend.storage.api.files.schemas import (  # noqa: E402
    IngestedObjectResponse,
    IngestionTaskResponse,
)
from src.cli.client import DataSourcesClient, StorageClient  # noqa: E402

NS_ID = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
GRP_ID = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000001")
OBJ_ID = uuid.UUID("cccccccc-0000-0000-0000-000000000001")
OWN_ID = uuid.UUID("dddddddd-0000-0000-0000-000000000001")
_NOW = datetime.datetime(2026, 6, 5, 12, 0, tzinfo=datetime.timezone.utc)


def make_source(**overrides) -> DataSourceResponse:
    defaults: dict = dict(
        id=str(GRP_ID),
        display_name="docs-watcher",
        source_type="filesystem",
        connector_name="artemis-bbbbbbbb",
        namespace_id=str(NS_ID),
        namespace_name="docs-ns",
        org_name="acme",
        owner_id=str(OWN_ID),
        config={"path": "/watch/docs"},
        created_at=_NOW.isoformat(),
        updated_at=_NOW.isoformat(),
        kafka_status=None,
    )
    defaults.update(overrides)
    return DataSourceResponse.model_validate(defaults)


def make_object(**overrides) -> IngestedObjectResponse:
    defaults: dict = dict(
        id=str(OBJ_ID),
        namespace_id=str(NS_ID),
        source="/watch/docs/dir1/report.pdf",
        object_type="document",
        content_type="application/pdf",
        size_bytes=1024,
        group_id=str(GRP_ID),
        ingested_at=_NOW.isoformat(),
    )
    defaults.update(overrides)
    return IngestedObjectResponse.model_validate(defaults)


def make_task(**overrides) -> IngestionTaskResponse:
    defaults: dict = dict(
        task_id=str(uuid.uuid4()),
        obj_id=str(OBJ_ID),
        namespace_id=str(NS_ID),
        status="SUCCESS",
        stage="tasks.index",
        operation="CREATE",
        failure_reason=None,
        created_at=_NOW.isoformat(),
        completed_at=_NOW.isoformat(),
    )
    defaults.update(overrides)
    return IngestionTaskResponse.model_validate(defaults)


@pytest.fixture
def mock_ds() -> AsyncMock:
    client = AsyncMock(spec=DataSourcesClient)
    client.list_sources.return_value = [make_source()]
    client.create_source.return_value = make_source()
    client.delete_source.return_value = None
    client.delete_namespace_sources.return_value = None
    return client


@pytest.fixture
def mock_storage() -> AsyncMock:
    client = AsyncMock(spec=StorageClient)
    client.list_objects.return_value = [make_object()]
    client.list_tasks.return_value = [make_task()]
    client.delete_object.return_value = None
    return client
