"""Real MCP client session through the real APISIX gateway.

The other two gateway suites each cover half of this path: test_smoke.py
proves APISIX forwards to *some* upstream (with backend-mcp absent, so it
never asserts anything about what comes back), and
tests/backend/mcp/integration/container/test_smoke.py drives a full MCP
client session but talks to backend-mcp directly, bypassing the gateway.
Neither would have caught a route registered as ``uris: ["/mcp", "/mcp/*"]``
mismatched against what the MCP SDK actually requests — this suite closes
that gap by running a real ``ClientSession.initialize()`` handshake through
APISIX to a real backend-mcp container.
"""

from __future__ import annotations

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from testcontainers.core.container import DockerContainer


@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_mcp_client_initializes_through_gateway(
    proxy_url: str,
    mcp_container: DockerContainer,
) -> None:
    async with streamable_http_client(f"{proxy_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            result = await session.initialize()
    assert result.serverInfo.name == "artemis"


@pytest.mark.integration
@pytest.mark.local
@pytest.mark.asyncio
async def test_mcp_client_calls_tool_through_gateway(
    proxy_url: str,
    mcp_container: DockerContainer,
) -> None:
    async with streamable_http_client(f"{proxy_url}/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("list_namespaces", {})
    assert not result.isError
    assert len(result.content) == 1
