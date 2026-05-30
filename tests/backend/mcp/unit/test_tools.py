# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for MCP tools.

All HTTP calls are intercepted by respx; no real service is required.
Tool functions are called directly as async functions with a stub Context —
this avoids standing up the full MCP protocol layer in unit tests.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock

import pytest
import respx
from httpx import Response

from src.backend.mcp.api import client as mcp_client
from src.backend.mcp.api.settings import settings
from src.backend.mcp.api.tools import (
    get_namespace,
    get_object,
    list_groups,
    list_namespaces,
    list_objects,
    retrieve,
    upload_file,
)

_OWNER_ID = settings.STUB_OWNER_ID

_NS_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_OBJ_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_GROUP_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"

_NAMESPACE = {"id": _NS_ID, "name": "test-ns", "type": "PRIVATE"}
_OBJECT = {"obj_id": _OBJ_ID, "filename": "doc.pdf", "content_type": "application/pdf"}
_CHUNKS = [
    {"page_content": "hello", "metadata": {"source": "doc.pdf", "score": 0.9}},
]


# ---------------------------------------------------------------------------
# Stub Context
# ---------------------------------------------------------------------------


def _ctx() -> AsyncMock:
    ctx = AsyncMock()
    ctx.info = AsyncMock()
    return ctx


# ---------------------------------------------------------------------------
# list_namespaces
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_namespaces():
    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        route = mock.get("/namespaces").mock(
            return_value=Response(200, json=[_NAMESPACE])
        )
        result = await list_namespaces(ctx=_ctx())

    assert result == [_NAMESPACE]
    sent = route.calls[0].request
    assert sent.headers["x-owner-id"] == _OWNER_ID


# ---------------------------------------------------------------------------
# get_namespace
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_namespace():
    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        route = mock.get(f"/namespaces/{_NS_ID}").mock(
            return_value=Response(200, json=_NAMESPACE)
        )
        result = await get_namespace(namespace_id=_NS_ID, ctx=_ctx())

    assert result == _NAMESPACE
    sent = route.calls[0].request
    assert sent.headers["x-owner-id"] == _OWNER_ID


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_objects_no_filter():
    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        mock.get(f"/namespaces/{_NS_ID}/objects").mock(
            return_value=Response(200, json=[_OBJECT])
        )
        result = await list_objects(namespace_id=_NS_ID, ctx=_ctx())

    assert result == [_OBJECT]


@pytest.mark.asyncio
async def test_list_objects_with_group_id():
    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        route = mock.get(f"/namespaces/{_NS_ID}/objects").mock(
            return_value=Response(200, json=[_OBJECT])
        )
        await list_objects(namespace_id=_NS_ID, group_id=_GROUP_ID, ctx=_ctx())

    sent = route.calls[0].request
    assert f"group_id={_GROUP_ID}" in str(sent.url)


# ---------------------------------------------------------------------------
# get_object
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_object():
    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        mock.get(f"/namespaces/{_NS_ID}/objects/{_OBJ_ID}").mock(
            return_value=Response(200, json=_OBJECT)
        )
        result = await get_object(namespace_id=_NS_ID, obj_id=_OBJ_ID, ctx=_ctx())

    assert result == _OBJECT


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_file():
    content = b"# Hello\n"
    encoded = base64.b64encode(content).decode()
    upload_response = {"obj_id": _OBJ_ID, "task_id": "task-123"}

    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        route = mock.post(f"/namespaces/{_NS_ID}/objects").mock(
            return_value=Response(202, json=upload_response)
        )
        result = await upload_file(
            namespace_id=_NS_ID,
            filename="hello.md",
            content_base64=encoded,
            content_type="text/markdown",
            ctx=_ctx(),
        )

    assert result == upload_response
    sent = route.calls[0].request
    assert sent.headers["x-owner-id"] == _OWNER_ID
    assert sent.headers["content-type"] == "text/markdown"
    assert sent.headers["x-filename"] == "hello.md"
    assert sent.content == content


# ---------------------------------------------------------------------------
# list_groups
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_groups():
    groups = [{"group_id": _GROUP_ID, "object_count": 3}]

    with respx.mock(base_url=mcp_client.storage_client.base_url) as mock:
        mock.get(f"/namespaces/{_NS_ID}/groups").mock(
            return_value=Response(200, json=groups)
        )
        result = await list_groups(namespace_id=_NS_ID, ctx=_ctx())

    assert result == groups


# ---------------------------------------------------------------------------
# retrieve
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retrieve_basic():
    langserve_response = {"output": _CHUNKS}

    with respx.mock(base_url=mcp_client.indexing_client.base_url) as mock:
        route = mock.post("/retrieve/invoke").mock(
            return_value=Response(200, json=langserve_response)
        )
        result = await retrieve(namespace_id=_NS_ID, query="hello world", ctx=_ctx())

    assert len(result) == 1
    assert result[0]["content"] == "hello"
    assert result[0]["source"] == "doc.pdf"
    assert result[0]["score"] == pytest.approx(0.9)

    sent_body = json.loads(route.calls[0].request.content)
    assert sent_body["input"] == "hello world"
    cfg = sent_body["config"]["configurable"]
    assert cfg["namespace_id"] == _NS_ID
    assert cfg["k"] == 5
    assert "group_id" not in cfg
    assert "doc_id" not in cfg


@pytest.mark.asyncio
async def test_retrieve_with_filters():
    langserve_response = {"output": _CHUNKS}

    with respx.mock(base_url=mcp_client.indexing_client.base_url) as mock:
        route = mock.post("/retrieve/invoke").mock(
            return_value=Response(200, json=langserve_response)
        )
        await retrieve(
            namespace_id=_NS_ID,
            query="test",
            top_k=3,
            group_id=_GROUP_ID,
            doc_id=_OBJ_ID,
            ctx=_ctx(),
        )

    sent_body = json.loads(route.calls[0].request.content)
    cfg = sent_body["config"]["configurable"]
    assert cfg["k"] == 3
    assert cfg["group_id"] == _GROUP_ID
    assert cfg["doc_id"] == _OBJ_ID
