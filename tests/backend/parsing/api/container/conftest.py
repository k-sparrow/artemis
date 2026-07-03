from __future__ import annotations

import time
from collections.abc import AsyncIterator

import httpx
import pytest
from minio import Minio
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

# Shard trigger/limit for the batch-mode test container.
# 50-page fixture PDF (sample-50-page-pdf-a4-size.pdf) is split into 2 shards
# of 25 pages each when DOCLING_SHARD_TRIGGER_PAGES=25.
_BATCH_SHARD_TRIGGER = 25
_BATCH_SHARD_LIMIT = 25

_DOCLING_IMAGE = "ghcr.io/docling-project/docling-serve-cu128:v1.24.0"
_DOCLING_PORT = 5001
_APP_IMAGE = "artemis/backend-parsing:dev"
_APP_PORT = 10001


def _with_s3_env(container: DockerContainer) -> DockerContainer:
    """Wire a container to the MinIO testcontainer (alias 'minio')."""
    return (
        container.with_env("S3_ENDPOINT", "minio:9000")
        .with_env("S3_ACCESS_KEY", "minioadmin")
        .with_env("S3_SECRET_KEY", "minioadmin")
        .with_env("S3_SECURE", "false")
    )


# ---------------------------------------------------------------------------
# Docker network
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_network():
    with Network() as network:
        yield network


# ---------------------------------------------------------------------------
# Infrastructure containers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def minio_container(
    docker_network: Network, request: pytest.FixtureRequest
) -> MinioContainer:
    container = (
        MinioContainer().with_network(docker_network).with_network_aliases("minio")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def minio_client(minio_container: MinioContainer) -> Minio:
    """Host-side MinIO client for reading the parse artifacts the service wrote."""
    return minio_container.get_client()


@pytest.fixture(scope="session")
def docling_container(
    docker_network: Network, request: pytest.FixtureRequest
) -> DockerContainer:
    container = (
        DockerContainer(_DOCLING_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("docling-serve")
        .with_exposed_ports(_DOCLING_PORT)
        .waiting_for(HttpWaitStrategy(_DOCLING_PORT, "/health").for_status_code(200))
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# App container
# ---------------------------------------------------------------------------


def _wait_for_liveness(url: str, timeout: int = 120, interval: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(url, timeout=3.0)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(
        f"Parsing service did not become ready at {url} within {timeout}s"
    )


@pytest.fixture(scope="session")
def app_container(
    docker_network: Network,
    docling_container: DockerContainer,
    minio_container: MinioContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    """The real parsing service image, wired to the Docling + MinIO testcontainers."""
    container = _with_s3_env(
        DockerContainer(_APP_IMAGE)
        .with_network(docker_network)
        .with_env("DOCLING_SERVE_URI", f"http://docling-serve:{_DOCLING_PORT}")
        .with_env("DEBUG", "true")
        .with_exposed_ports(_APP_PORT)
    )
    container.start()
    request.addfinalizer(container.stop)

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(_APP_PORT))
    _wait_for_liveness(f"http://{host}:{port}/health/liveness")
    return container


@pytest.fixture(scope="session")
def app_base_url(app_container: DockerContainer) -> str:
    host = app_container.get_container_host_ip()
    port = int(app_container.get_exposed_port(_APP_PORT))
    return f"http://{host}:{port}"


@pytest.fixture
async def client(app_base_url: str) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=app_base_url, timeout=300.0) as c:
        yield c


# ---------------------------------------------------------------------------
# App container with a dead Docling URI (for 503 / upstream-down tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def app_container_no_docling(
    docker_network: Network,
    minio_container: MinioContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    """Parsing service started with a Docling URI that is guaranteed unreachable.

    Uses TEST-NET (192.0.2.x, RFC 5737) which is non-routable by definition,
    so any HTTP call to it will time-out or be refused without side effects.
    """
    container = _with_s3_env(
        DockerContainer(_APP_IMAGE)
        .with_network(docker_network)
        .with_env("DOCLING_SERVE_URI", "http://docling-does-not-exist.invalid:5001")
        .with_env("DEBUG", "true")
        .with_exposed_ports(_APP_PORT)
    )
    container.start()
    request.addfinalizer(container.stop)

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(_APP_PORT))
    _wait_for_liveness(f"http://{host}:{port}/health/liveness")
    return container


@pytest.fixture(scope="session")
def app_base_url_no_docling(app_container_no_docling: DockerContainer) -> str:
    host = app_container_no_docling.get_container_host_ip()
    port = int(app_container_no_docling.get_exposed_port(_APP_PORT))
    return f"http://{host}:{port}"


@pytest.fixture
async def client_no_docling(
    app_base_url_no_docling: str,
) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=app_base_url_no_docling, timeout=30.0) as c:
        yield c


# ---------------------------------------------------------------------------
# App container with low shard threshold (for batch-mode integration tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def app_container_batch(
    docker_network: Network,
    docling_container: DockerContainer,
    minio_container: MinioContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    """Parsing service with a low shard threshold so that the 50-page fixture
    PDF triggers the batch S3 conversion path (2 shards of 25 pages each)."""
    container = _with_s3_env(
        DockerContainer(_APP_IMAGE)
        .with_network(docker_network)
        .with_env("DOCLING_SERVE_URI", f"http://docling-serve:{_DOCLING_PORT}")
        .with_env("DOCLING_SHARD_TRIGGER_PAGES", str(_BATCH_SHARD_TRIGGER))
        .with_env("DOCLING_SHARD_PAGE_LIMIT", str(_BATCH_SHARD_LIMIT))
        .with_env("DEBUG", "true")
        .with_exposed_ports(_APP_PORT)
    )
    container.start()
    request.addfinalizer(container.stop)

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(_APP_PORT))
    _wait_for_liveness(f"http://{host}:{port}/health/liveness")
    return container


@pytest.fixture(scope="session")
def app_base_url_batch(app_container_batch: DockerContainer) -> str:
    host = app_container_batch.get_container_host_ip()
    port = int(app_container_batch.get_exposed_port(_APP_PORT))
    return f"http://{host}:{port}"


@pytest.fixture
async def client_batch(app_base_url_batch: str) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=app_base_url_batch, timeout=600.0) as c:
        yield c
