"""Integration fixtures for the APISIX gateway smoke tests.

Starts a real APISIX + etcd stack, runs the init container to register routes,
then exposes the proxy URL and Admin API URL for assertions.

Images required before running:
    bazel run //tools/oci/images/apisix:init_tarball
    docker pull apache/apisix:3.16.0-debian
    docker pull bitnami/etcd:3.5.18
"""

from __future__ import annotations

import os
import tempfile

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from wiremock.testing.testcontainer import WireMockContainer

from tests.lib.polling import wait_for_http

_ETCD_IMAGE = "gcr.io/etcd-development/etcd:v3.5.18"
_APISIX_IMAGE = "apache/apisix:3.16.0-debian"
_INIT_IMAGE = "artemis/apisix-init:latest"
_MCP_IMAGE = "artemis/backend-mcp:dev"
_MCP_PORT = 11000
_ADMIN_KEY = "artemis-apisix-admin-key"
_NS_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


_APISIX_CONFIG = """\
deployment:
  role: traditional
  traditional:
    config_provider: etcd
  etcd:
    host:
      - "http://apisix-etcd:2379"
    prefix: /apisix
    timeout: 30
  admin:
    admin_key_required: false
    allow_admin:
      - 0.0.0.0/0
    admin_listen:
      ip: 0.0.0.0
      port: 9180
apisix:
  node_listen: 9080
"""


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


@pytest.fixture(scope="session")
def apisix_config_file(request: pytest.FixtureRequest) -> str:
    """Write APISIX config to a real /tmp path Docker can reach."""
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="apisix-test-", delete=False
    )
    f.write(_APISIX_CONFIG)
    f.close()
    os.chmod(f.name, 0o666)
    request.addfinalizer(lambda: os.unlink(f.name))
    return f.name


@pytest.fixture(scope="session")
def apisix_etcd(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> DockerContainer:
    container = (
        DockerContainer(_ETCD_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("apisix-etcd")
        .with_command(
            "etcd "
            "--listen-client-urls http://0.0.0.0:2379 "
            "--advertise-client-urls http://apisix-etcd:2379 "
            "--data-dir /tmp/etcd"
        )
        .with_exposed_ports(2379)
    )
    container.start()
    request.addfinalizer(container.stop)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(2379)
    wait_for_http(f"http://{host}:{port}/health", timeout=30)
    return container


@pytest.fixture(scope="session")
def apisix(
    request: pytest.FixtureRequest,
    docker_network: Network,
    apisix_etcd: DockerContainer,
    apisix_config_file: str,
) -> DockerContainer:
    container = (
        DockerContainer(_APISIX_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("apisix")
        .with_volume_mapping(
            apisix_config_file,
            "/usr/local/apisix/conf/config.yaml",
            "rw",
        )
        .with_exposed_ports(9080, 9180)
    )
    container.start()
    request.addfinalizer(container.stop)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(9180)
    try:
        wait_for_http(f"http://{host}:{port}/apisix/admin/routes", timeout=60)
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== apisix stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== apisix stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container


@pytest.fixture(scope="session")
def apisix_init(
    request: pytest.FixtureRequest,
    docker_network: Network,
    apisix: DockerContainer,
) -> None:
    """Run the init container to register routes; wait for clean exit."""
    container = (
        DockerContainer(_INIT_IMAGE)
        .with_network(docker_network)
        .with_env("APISIX_ADMIN_URL", "http://apisix:9180")
        .with_env("APISIX_ADMIN_KEY", _ADMIN_KEY)
    )
    container.start()
    request.addfinalizer(container.stop)
    result = container.get_wrapped_container().wait(timeout=60)
    assert result["StatusCode"] == 0, (
        "apisix-init exited non-zero:\n"
        + container.get_logs()[0].decode(errors="replace")
    )


@pytest.fixture(scope="session")
def proxy_url(apisix: DockerContainer, apisix_init: None) -> str:
    host = apisix.get_container_host_ip()
    port = apisix.get_exposed_port(9080)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def admin_url(apisix: DockerContainer, apisix_init: None) -> str:
    host = apisix.get_container_host_ip()
    port = apisix.get_exposed_port(9180)
    return f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# backend-mcp — real service behind the gateway's "mcp" route, with its own
# storage/indexing dependencies stubbed by WireMock. Lets tests drive an
# actual MCP client session through APISIX end to end.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mcp_wiremock(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> WireMockContainer:
    wm = WireMockContainer(secure=False)
    wm.with_network(docker_network)
    wm.with_network_aliases("mcp-wiremock")
    wm.with_mapping(
        "health-liveness.json",
        {
            "request": {"method": "GET", "url": "/health/liveness"},
            "response": {"status": 200},
        },
    )
    wm.with_mapping(
        "health-readiness.json",
        {
            "request": {"method": "GET", "url": "/health/readiness"},
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
                        "owner_id": "00000000-0000-0000-0000-000000000000",
                    }
                ],
            },
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def mcp_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    mcp_wiremock: WireMockContainer,
) -> DockerContainer:
    """The backend-mcp service, aliased ``backend-mcp`` on ``docker_network`` —
    the exact hostname the apisix-init route registers as its upstream node,
    so the gateway's proxied requests resolve to this container."""
    wiremock_url = f"http://mcp-wiremock:{mcp_wiremock.http_server_port}"
    container = (
        DockerContainer(_MCP_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("backend-mcp")
        .with_env("STORAGE_SERVICE_URL", wiremock_url)
        .with_env("INDEXING_SERVICE_URL", wiremock_url)
        .with_env("ENABLE_UPLOAD", "false")
        .with_exposed_ports(_MCP_PORT)
    )
    container.start()
    request.addfinalizer(container.stop)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_MCP_PORT)
    try:
        wait_for_http(f"http://{host}:{port}/health/liveness", timeout=90)
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== backend-mcp stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== backend-mcp stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container
