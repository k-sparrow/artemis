"""Contract test fixtures for the APISIX init script.

Starts a WireMock container that stubs the APISIX Admin API, then runs the
artemis/apisix-init:latest one-shot container against it. After the container
exits the WireMock request journal is exposed for assertions.

The init image must be pre-built before running:
    bazel run //tools/oci/images/apisix:init_tarball
"""

from __future__ import annotations


import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from wiremock.testing.testcontainer import WireMockContainer

_INIT_IMAGE = "artemis/apisix-init:latest"
_ADMIN_KEY = "artemis-apisix-admin-key"


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


@pytest.fixture(scope="session")
def wiremock(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> WireMockContainer:
    wm = WireMockContainer(secure=False)
    wm.with_network(docker_network)
    wm.with_network_aliases("apisix-admin-stub")
    # Init script polls GET /apisix/admin/routes to wait for APISIX readiness
    wm.with_mapping(
        "admin-health.json",
        {
            "request": {"method": "GET", "url": "/apisix/admin/routes"},
            "response": {"status": 200, "jsonBody": {"list": [], "total": 0}},
        },
    )
    wm.with_mapping(
        "put-mcp-route.json",
        {
            "request": {"method": "PUT", "url": "/apisix/admin/routes/mcp"},
            "response": {"status": 201, "jsonBody": {}},
        },
    )
    wm.with_mapping(
        "put-data-sources-route.json",
        {
            "request": {"method": "PUT", "url": "/apisix/admin/routes/data-sources"},
            "response": {"status": 201, "jsonBody": {}},
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def received_requests(
    request: pytest.FixtureRequest,
    docker_network: Network,
    wiremock: WireMockContainer,
) -> list[dict]:
    """Run the init container against the WireMock Admin API stub and return
    the full WireMock request journal for contract assertions."""
    wm_internal_url = f"http://apisix-admin-stub:{wiremock.http_server_port}"
    container = (
        DockerContainer(_INIT_IMAGE)
        .with_network(docker_network)
        .with_env("APISIX_ADMIN_URL", wm_internal_url)
        .with_env("APISIX_ADMIN_KEY", _ADMIN_KEY)
    )
    container.start()
    request.addfinalizer(container.stop)

    result = container.get_wrapped_container().wait(timeout=60)
    assert result["StatusCode"] == 0, (
        "apisix-init exited non-zero:\n"
        + container.get_logs()[0].decode(errors="replace")
    )

    host = wiremock.get_container_host_ip()
    port = wiremock.get_exposed_port(wiremock.http_server_port)
    resp = httpx.get(f"http://{host}:{port}/__admin/requests")
    resp.raise_for_status()
    return resp.json()["requests"]
