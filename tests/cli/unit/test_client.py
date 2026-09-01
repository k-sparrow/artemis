"""Unit tests for DataSourcesClient — verifies correct HTTP requests via respx mocks."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import httpx
import pytest
import respx
from httpx import Response

from src.backend.enterprise.data_sources.api.sources.schemas import (
    DataSourceCreate,
    DataSourceResponse,
)
from src.backend.storage.api.files.schemas import IngestionTaskResponse
from src.cli.client import DataSourcesClient, StorageClient

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_GATEWAY = "http://gateway-test:9080"

_SOURCE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_NS_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_NOW = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)

_OWNER_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")

_SAMPLE_RESPONSE = {
    "id": str(_SOURCE_ID),
    "display_name": "docs-watcher",
    "source_type": "filesystem",
    "connector_name": "artemis-aaaaaaaa",
    "namespace_id": str(_NS_ID),
    "namespace_name": "docs-namespace",
    "org_name": "acme",
    "owner_id": str(_OWNER_ID),
    "config": {"path": "/data/docs"},
    "created_at": _NOW.isoformat(),
    "updated_at": _NOW.isoformat(),
    "kafka_status": {"state": "RUNNING", "worker_id": "worker-0:8083", "tasks": []},
}


@pytest.fixture
def client():
    return DataSourcesClient(base_url=_GATEWAY)


# ---------------------------------------------------------------------------
# list_sources
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_sources_returns_parsed_list(client):
    with respx.mock(base_url=_GATEWAY) as mock:
        mock.get("/data-sources").mock(
            return_value=Response(200, json=[_SAMPLE_RESPONSE])
        )
        result = await client.list_sources()

    assert len(result) == 1
    assert result[0].display_name == "docs-watcher"
    assert result[0].namespace_name == "docs-namespace"
    assert result[0].kafka_status.state == "RUNNING"


@pytest.mark.asyncio
async def test_list_sources_raises_on_error(client):
    with respx.mock(base_url=_GATEWAY) as mock:
        mock.get("/data-sources").mock(return_value=Response(502))
        with pytest.raises(Exception):
            await client.list_sources()


# ---------------------------------------------------------------------------
# get_source
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_source_constructs_correct_url(client):
    with respx.mock(base_url=_GATEWAY) as mock:
        route = mock.get(f"/data-sources/{_SOURCE_ID}").mock(
            return_value=Response(200, json=_SAMPLE_RESPONSE)
        )
        result = await client.get_source(_SOURCE_ID)

    assert route.called
    assert result.id == _SOURCE_ID


@pytest.mark.asyncio
async def test_get_source_raises_on_404(client):
    with respx.mock(base_url=_GATEWAY) as mock:
        mock.get(f"/data-sources/{_SOURCE_ID}").mock(return_value=Response(404))
        with pytest.raises(Exception):
            await client.get_source(_SOURCE_ID)


# ---------------------------------------------------------------------------
# create_source
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_source_posts_json_body(client):
    payload = DataSourceCreate(
        display_name="docs-watcher",
        namespace="docs-namespace",
        org_name="acme",
        path="/data/docs",
        recursive=True,
    )
    with respx.mock(base_url=_GATEWAY) as mock:
        route = mock.post("/data-sources").mock(
            return_value=Response(201, json=_SAMPLE_RESPONSE)
        )
        result = await client.create_source(payload)

    assert route.called
    sent = route.calls[0].request
    import json

    body = json.loads(sent.content)
    assert body["display_name"] == "docs-watcher"
    assert body["namespace"] == "docs-namespace"
    assert result.id == _SOURCE_ID


# ---------------------------------------------------------------------------
# delete_source
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_source_sends_delete(client):
    with respx.mock(base_url=_GATEWAY) as mock:
        route = mock.delete(f"/data-sources/{_SOURCE_ID}").mock(
            return_value=Response(204)
        )
        await client.delete_source(_SOURCE_ID)

    assert route.called


# ---------------------------------------------------------------------------
# pause / resume / restart
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["pause", "resume", "restart"])
@pytest.mark.asyncio
async def test_lifecycle_action_posts_and_returns_response(action, client):
    with respx.mock(base_url=_GATEWAY) as mock:
        route = mock.post(f"/data-sources/{_SOURCE_ID}/{action}").mock(
            return_value=Response(200, json=_SAMPLE_RESPONSE)
        )
        method = getattr(client, f"{action}_source")
        result = await method(_SOURCE_ID)

    assert route.called
    assert isinstance(result, DataSourceResponse)
    assert result.id == _SOURCE_ID


# ---------------------------------------------------------------------------
# StorageClient.list_tasks
# ---------------------------------------------------------------------------

_TASK_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
_OBJ_ID = uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")

_SAMPLE_TASK_RESPONSE = {
    "task_id": str(_TASK_ID),
    "obj_id": str(_OBJ_ID),
    "namespace_id": str(_NS_ID),
    "status": "running",
    "stage": "tasks.submit_parse",
    "operation": "CREATE",
    "failure_reason": None,
    "created_at": _NOW.isoformat(),
    "completed_at": None,
}


@pytest.fixture
def storage_client():
    return StorageClient(base_url=_GATEWAY)


@pytest.mark.asyncio
async def test_list_tasks_returns_parsed_list(storage_client):
    with respx.mock(base_url=_GATEWAY) as mock:
        mock.get(f"/namespaces/{_NS_ID}/tasks").mock(
            return_value=Response(200, json=[_SAMPLE_TASK_RESPONSE])
        )
        result = await storage_client.list_tasks(_NS_ID, _OWNER_ID)

    assert result == [IngestionTaskResponse.model_validate(_SAMPLE_TASK_RESPONSE)]


@pytest.mark.asyncio
async def test_list_tasks_sends_owner_header(storage_client):
    with respx.mock(base_url=_GATEWAY) as mock:
        route = mock.get(f"/namespaces/{_NS_ID}/tasks").mock(
            return_value=Response(200, json=[_SAMPLE_TASK_RESPONSE])
        )
        await storage_client.list_tasks(_NS_ID, _OWNER_ID)

    assert route.called
    assert route.calls[0].request.headers["X-Owner-Id"] == str(_OWNER_ID)


@pytest.mark.asyncio
async def test_list_tasks_surfaces_running_status_and_stage(storage_client):
    """The CLI/TUI's whole reason for polling this endpoint (Epic 22): a
    still-running task must come back with its live stage and a null
    completed_at, not just terminal SUCCESS/FAILURE rows."""
    with respx.mock(base_url=_GATEWAY) as mock:
        mock.get(f"/namespaces/{_NS_ID}/tasks").mock(
            return_value=Response(200, json=[_SAMPLE_TASK_RESPONSE])
        )
        result = await storage_client.list_tasks(_NS_ID, _OWNER_ID)

    assert result[0].status == "running"
    assert result[0].stage == "tasks.submit_parse"
    assert result[0].completed_at is None


@pytest.mark.asyncio
async def test_list_tasks_raises_on_error(storage_client):
    with respx.mock(base_url=_GATEWAY) as mock:
        mock.get(f"/namespaces/{_NS_ID}/tasks").mock(return_value=Response(404))
        with pytest.raises(httpx.HTTPStatusError):
            await storage_client.list_tasks(_NS_ID, _OWNER_ID)
