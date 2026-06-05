"""Container integration fixtures for the MCP server.

Starts the real ``artemis/backend-mcp:dev`` image alongside a WireMock
container that stubs both the storage service and the indexing service.
No real databases or vector stores are required — the MCP server is stateless
and delegates every operation to those two upstreams.

The MCP container must be pre-built before running:
    bazel run //src/backend/mcp/api:tarball.dev
"""

from __future__ import annotations

import time

import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from wiremock.testing.testcontainer import WireMockContainer

_MCP_IMAGE = "artemis/backend-mcp:dev"
_MCP_PORT = 11000
_STUB_OWNER_ID = "00000000-0000-0000-0000-000000000000"
_NS_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def _wait_for_http(base_url: str, path: str, timeout: int = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(f"{base_url}{path}", timeout=3.0)
            if resp.status_code == 200:
                return
        except httpx.RequestError:
            pass
        time.sleep(2)
    raise TimeoutError(f"{base_url}{path} did not become ready within {timeout}s")


# ---------------------------------------------------------------------------
# Docker network
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


# ---------------------------------------------------------------------------
# WireMock — stubs both storage and indexing upstream services
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def wiremock(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> WireMockContainer:
    wm = WireMockContainer(secure=False)
    wm.with_network(docker_network)
    wm.with_network_aliases("wiremock")
    wm.with_mapping(
        "health-liveness.json",
        {
            "request": {"method": "GET", "url": "/health/liveness"},
            "response": {"status": 200},
        },
    )
    wm.with_mapping(
        "get-namespaces.json",
        {
            "request": {"method": "GET", "url": "/namespaces"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": [
                    {
                        "id": _NS_ID,
                        "name": "smoke-ns",
                        "type": "SHARED",
                        "owner_id": _STUB_OWNER_ID,
                    }
                ],
            },
        },
    )
    wm.with_mapping(
        "post-upload-object.json",
        {
            "request": {
                "method": "POST",
                "urlPattern": f"/namespaces/{_NS_ID}/objects",
            },
            "response": {
                "status": 202,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {
                    "obj_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "task_id": "tttttttt-tttt-tttt-tttt-tttttttttttt",
                },
            },
        },
    )
    wm.with_mapping(
        "post-retrieve-invoke.json",
        {
            "request": {"method": "POST", "url": "/retrieve/invoke"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {"output": []},
            },
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


# ---------------------------------------------------------------------------
# MCP service container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mcp_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    wiremock: WireMockContainer,
) -> DockerContainer:
    wiremock_url = f"http://wiremock:{wiremock.http_server_port}"
    container = (
        DockerContainer(_MCP_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("backend-mcp")
        .with_env("STORAGE_SERVICE_URL", wiremock_url)
        .with_env("INDEXING_SERVICE_URL", wiremock_url)
        .with_env("STUB_OWNER_ID", _STUB_OWNER_ID)
        .with_env("ENABLE_UPLOAD", "true")
        .with_env("DEBUG", "true")
        .with_exposed_ports(_MCP_PORT)
    )
    container.start()
    request.addfinalizer(container.stop)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_MCP_PORT)
    try:
        _wait_for_http(f"http://{host}:{port}", "/health/liveness")
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== backend-mcp stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== backend-mcp stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container


@pytest.fixture(scope="session")
def mcp_base_url(mcp_container: DockerContainer) -> str:
    host = mcp_container.get_container_host_ip()
    port = mcp_container.get_exposed_port(_MCP_PORT)
    return f"http://{host}:{port}"
