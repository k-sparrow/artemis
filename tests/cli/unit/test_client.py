"""Unit tests for DataSourcesClient — verifies correct HTTP requests via respx mocks."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
import respx
from httpx import Response

from src.backend.enterprise.data_sources.api.sources.schemas import (
    DataSourceCreate,
    DataSourceResponse,
)
from src.cli.client import DataSourcesClient

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
