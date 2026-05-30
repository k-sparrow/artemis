"""Container smoke tests for the MCP server.

Starts the real ``artemis/backend-mcp:dev`` image with WireMock stubbing both
upstream services (storage + indexing). Tests exercise the full MCP protocol
stack — initialize handshake, tool discovery, and tool invocation — without
any real databases or vector stores.

FastMCP serializes list[dict] return values as one TextContent block per dict
element (not as a single JSON array). An empty list produces empty content.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

_NS_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

_EXPECTED_TOOLS = {
    "list_namespaces",
    "get_namespace",
    "list_objects",
    "get_object",
    "upload_file",
    "list_groups",
    "retrieve",
}


# ---------------------------------------------------------------------------
# Liveness
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_liveness_returns_200(mcp_base_url: str) -> None:
    async with httpx.AsyncClient(base_url=mcp_base_url) as client:
        resp = await client.get("/health/liveness")
    assert resp.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readiness_returns_200(mcp_base_url: str) -> None:
    async with httpx.AsyncClient(base_url=mcp_base_url) as client:
        resp = await client.get("/health/readiness")
    assert resp.status_code == 200, resp.json()


# ---------------------------------------------------------------------------
# MCP protocol — initialize + tool discovery
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_initialize_reports_server_name(mcp_base_url: str) -> None:
    async with streamable_http_client(f"{mcp_base_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            result = await session.initialize()
    assert result.serverInfo.name == "artemis"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_tools_exposes_all_seven_tools(mcp_base_url: str) -> None:
    async with streamable_http_client(f"{mcp_base_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools_result = await session.list_tools()
    tool_names = {t.name for t in tools_result.tools}
    assert _EXPECTED_TOOLS == tool_names


# ---------------------------------------------------------------------------
# Tool invocations (backed by WireMock stubs)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_namespaces_returns_stubbed_namespace(mcp_base_url: str) -> None:
    """list_namespaces calls GET /namespaces on the storage service.
    WireMock returns one shared namespace.

    FastMCP serializes list[dict] as one TextContent block per element,
    so one namespace → one content block with the namespace JSON.
    """
    async with streamable_http_client(f"{mcp_base_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("list_namespaces", {})

    assert not result.isError
    assert len(result.content) == 1
    ns = json.loads(result.content[0].text)
    assert ns["id"] == _NS_ID
    assert ns["name"] == "smoke-ns"
    assert ns["type"] == "SHARED"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upload_file_returns_task_id(mcp_base_url: str) -> None:
    """upload_file POSTs to /namespaces/{id}/objects on the storage service.
    WireMock returns a 202 with obj_id and task_id."""
    content = base64.b64encode(b"# Hello\n").decode()
    async with streamable_http_client(f"{mcp_base_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "upload_file",
                {
                    "namespace_id": _NS_ID,
                    "filename": "hello.md",
                    "content_base64": content,
                    "content_type": "text/markdown",
                },
            )

    assert not result.isError
    assert len(result.content) == 1
    body = json.loads(result.content[0].text)
    assert "obj_id" in body
    assert "task_id" in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retrieve_returns_empty_results(mcp_base_url: str) -> None:
    """retrieve posts to /retrieve/invoke on the indexing service.
    WireMock returns {"output": []} — the tool returns [], which FastMCP
    serializes as empty content (one TextContent per chunk, none here).
    """
    async with streamable_http_client(f"{mcp_base_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "retrieve",
                {"namespace_id": _NS_ID, "query": "smoke test query"},
            )

    assert not result.isError
    assert result.content == []
