from __future__ import annotations

import time
from pathlib import Path
from collections.abc import AsyncIterator

import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from testcontainers.postgres import PostgresContainer
from testcontainers.qdrant import QdrantContainer

from tests.lib.testcontainers.tei import TEIContainer

_EMBEDDING_MODEL = "sentence-transformers/msmarco-MiniLM-L-12-v3"
_APP_IMAGE = "artemis/backend-indexing:dev"
_APP_PORT = 10000


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
def qdrant_container(
    docker_network: Network, request: pytest.FixtureRequest
) -> QdrantContainer:
    container = (
        QdrantContainer("qdrant/qdrant:v1.17")
        .with_network(docker_network)
        .with_network_aliases("vectorstore")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def tei_container(
    docker_network: Network, request: pytest.FixtureRequest
) -> TEIContainer:
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = (
        TEIContainer(model_id=_EMBEDDING_MODEL)
        .with_network(docker_network)
        .with_network_aliases("tei")
    )
    if hf_cache.exists():
        container.with_hf_cache(str(hf_cache))
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def postgres_container(
    docker_network: Network, request: pytest.FixtureRequest
) -> PostgresContainer:
    container = (
        PostgresContainer("postgres:16-alpine", driver="asyncpg")
        .with_network(docker_network)
        .with_network_aliases("postgres")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# App container
# ---------------------------------------------------------------------------


def _wait_for_liveness(url: str, timeout: int = 120000, interval: float = 2.0) -> None:
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
        f"Indexing service did not become ready at {url} within {timeout}s"
    )


@pytest.fixture(scope="session")
def app_container(
    docker_network: Network,
    qdrant_container: QdrantContainer,
    tei_container: TEIContainer,
    postgres_container: PostgresContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    """The real indexing service image, wired to testcontainer dependencies."""
    container = (
        DockerContainer(_APP_IMAGE)
        .with_network(docker_network)
        .with_env("QDRANT_HOST_URL", "http://vectorstore:6333")
        .with_env("QDRANT_COLLECTION_NAME", "test_collection")
        .with_env("TEI_HOST_URL", "http://tei:80")
        .with_env("SQL_DB_USER", postgres_container.username)
        .with_env("SQL_DB_PASSWORD", postgres_container.password)
        .with_env("SQL_DB_HOST", "postgres")
        .with_env("SQL_DB_PORT", "5432")
        .with_env("SQL_DB_DATABASE", postgres_container.dbname)
        .with_env("SQL_DRIVER", "postgresql+asyncpg")
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
