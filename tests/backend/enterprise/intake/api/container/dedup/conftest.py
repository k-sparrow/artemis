"""Container fixtures for the intake service's content-addressed dedup ledger.

Real Docker containers on a shared bridge network:
  - PostgresContainer — intake_dedup_ledger table
  - WireMockContainer — stubs the storage service
  - DockerContainer (artemis/backend-enterprise-intake:dev) — system under test

The intake service container must be pre-built before running:
    bazel run //src/backend/enterprise/intake/api:tarball.dev

Session-scoped: Postgres, WireMock, intake service, watch_dir (the ledger's
identity is keyed on canonical filesystem paths, so files must persist across
tests within a session for the cross-test "already claimed" assertions to be
meaningful — each test uses uniquely-named files/subdirs to avoid collisions).
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Iterator

import httpx
import pytest
from fastapi import status
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer
from wiremock.testing.testcontainer import WireMockContainer

_INTAKE_IMAGE = "artemis/backend-enterprise-intake:dev"
_INTAKE_PORT = 9000
_NAMESPACE_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_OWNER_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
_TASK_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"


def _wait_for_http(base_url: str, path: str, timeout: int = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(f"{base_url}{path}", timeout=3.0)
            if resp.status_code == status.HTTP_200_OK:
                return
        except httpx.RequestError:
            pass
        time.sleep(2)
    raise TimeoutError(f"{base_url}{path} did not become ready within {timeout}s")


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


@pytest.fixture(scope="session")
def postgres_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> PostgresContainer:
    container = (
        PostgresContainer(
            image="postgres:16-alpine",
            username="postgres",
            password="postgres",
            dbname="intake_dedup_test",
        )
        .with_network(docker_network)
        .with_network_aliases("postgres")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def migrations_container(
    docker_network: Network,
    postgres_container: PostgresContainer,
) -> None:
    from tests.lib.testcontainers.migrations import run_migrations_on_network

    run_migrations_on_network(
        docker_network,
        postgres_container.username,
        postgres_container.password,
        postgres_container.dbname,
    )


@pytest.fixture(scope="session")
def wiremock(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> WireMockContainer:
    wm = WireMockContainer(secure=False)
    wm.with_network(docker_network)
    wm.with_network_aliases("wiremock")
    wm.with_mapping(
        "namespace-get.json",
        {
            "request": {"method": "GET", "urlPattern": "/namespaces/.*"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {
                    "id": _NAMESPACE_ID,
                    "type": "shared",
                    "name": "dedup-test",
                    "owner_id": _OWNER_ID,
                },
            },
        },
    )
    wm.with_mapping(
        "objects-upload.json",
        {
            "request": {"method": "POST", "urlPattern": "/namespaces/.*/objects"},
            "response": {
                "status": 202,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {"task_id": _TASK_ID},
            },
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def watch_dir(request: pytest.FixtureRequest) -> Iterator[Path]:
    """Real host directory, mounted RO into the intake container at /watch.

    tempfile.mkdtemp(), not tmp_path — Docker needs to see the real host
    filesystem, which Bazel's sandboxed /tmp is not.
    """
    import shutil

    d = Path(tempfile.mkdtemp(prefix="intake-dedup-watch-"))
    request.addfinalizer(lambda: shutil.rmtree(d, ignore_errors=True))
    return d


@pytest.fixture(scope="session")
def intake_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    postgres_container: PostgresContainer,
    migrations_container: None,
    wiremock: WireMockContainer,
    watch_dir: Path,
) -> DockerContainer:
    wiremock_internal_url = f"http://wiremock:{wiremock.http_server_port}"
    container = (
        DockerContainer(_INTAKE_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("intake")
        .with_exposed_ports(_INTAKE_PORT)
        .with_env(
            "SQL_DB_URL",
            f"postgresql+asyncpg://{postgres_container.username}:{postgres_container.password}"  # noqa: E501
            f"@postgres:5432/{postgres_container.dbname}",
        )
        .with_env("STORAGE_SERVICE_URL", wiremock_internal_url)
        .with_env("DEBUG", "true")
        .with_volume_mapping(str(watch_dir), "/watch", "ro")
    )
    container.start()
    request.addfinalizer(container.stop)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(_INTAKE_PORT)
    try:
        _wait_for_http(f"http://{host}:{port}", "/health/liveness")
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== intake stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== intake stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container


@pytest.fixture(scope="session")
def intake_base_url(intake_container: DockerContainer) -> str:
    host = intake_container.get_container_host_ip()
    port = intake_container.get_exposed_port(_INTAKE_PORT)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def wiremock_admin_url(wiremock: WireMockContainer) -> str:
    return f"http://{wiremock.get_container_host_ip()}:{wiremock.get_exposed_port(wiremock.http_server_port)}"  # noqa: E501
