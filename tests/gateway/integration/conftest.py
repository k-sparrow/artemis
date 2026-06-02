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
from tests.lib.polling import wait_for_http

_ETCD_IMAGE = "gcr.io/etcd-development/etcd:v3.5.18"
_APISIX_IMAGE = "apache/apisix:3.16.0-debian"
_INIT_IMAGE = "artemis/apisix-init:latest"
_ADMIN_KEY = "artemis-apisix-admin-key"

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
