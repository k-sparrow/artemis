from __future__ import annotations

import time
from pathlib import Path
from collections.abc import AsyncIterator

import httpx
import pytest
from minio import Minio
from qdrant_client import AsyncQdrantClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.qdrant import QdrantContainer

from tests.lib.testcontainers.tei import TEIContainer
from tests.lib.testcontainers.vllm import VLLMContainer

_EMBEDDING_MODEL = "sentence-transformers/msmarco-MiniLM-L-12-v3"
_COLBERT_MODEL = "colbert-ir/colbertv2.0"
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
# Retrieval mode parametrization
# ---------------------------------------------------------------------------


@pytest.fixture(
    scope="session",
    params=["dense", "hybrid", "multi_stage"],
    ids=["dense", "hybrid", "multi_stage"],
)
def retrieval_mode(request: pytest.FixtureRequest) -> str:
    """Parametrize the indexing service over all retrieval modes.

    multi_stage requires a GPU-backed vLLM container (local tag).
    """
    return request.param


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
    """Host-side MinIO client for seeding parse artifacts the service reads."""
    return minio_container.get_client()


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
def vllm_container(
    retrieval_mode: str,
    docker_network: Network,
    request: pytest.FixtureRequest,
) -> VLLMContainer | None:
    """vLLM container serving ColBERT — started only for multi_stage mode."""
    if retrieval_mode != "multi_stage":
        return None
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = (
        VLLMContainer(model_id=_COLBERT_MODEL)
        .with_network(docker_network)
        .with_network_aliases("colbert")
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
        PostgresContainer(
            "postgres:16-alpine",
            username="postgres",
            password="postgres",
            driver="asyncpg",
        )
        .with_network(docker_network)
        .with_network_aliases("postgres")
        .with_command(
            "postgres "
            "-c wal_level=logical "
            "-c max_wal_senders=10 "
            "-c max_replication_slots=10"
        )
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
def collection_name(retrieval_mode: str) -> str:
    """Qdrant collection name, scoped per retrieval mode to avoid schema conflicts."""
    return f"test_collection_{retrieval_mode}"


@pytest.fixture(scope="session")
def app_container(
    docker_network: Network,
    qdrant_container: QdrantContainer,
    tei_container: TEIContainer,
    vllm_container: VLLMContainer | None,
    migrations_container: None,
    postgres_container: PostgresContainer,
    minio_container: MinioContainer,
    retrieval_mode: str,
    collection_name: str,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    """The real indexing service image, wired to testcontainer dependencies.

    One container instance per retrieval mode — ``retrieval_mode`` drives the
    parametrization so every test runs in dense, hybrid, and multi_stage
    configurations.  multi_stage requires a GPU-backed vLLM container.
    """
    container = (
        DockerContainer(_APP_IMAGE)
        .with_network(docker_network)
        .with_env("QDRANT_HOST_URL", "http://vectorstore:6333")
        .with_env("QDRANT_COLLECTION_NAME", collection_name)
        .with_env("TEI_HOST_URL", "http://tei:80")
        .with_env("RETRIEVAL_MODE", retrieval_mode)
        .with_env("SQL_DB_USER", postgres_container.username)
        .with_env("SQL_DB_PASSWORD", postgres_container.password)
        .with_env("SQL_DB_HOST", "postgres")
        .with_env("SQL_DB_PORT", "5432")
        .with_env("SQL_DB_DATABASE", postgres_container.dbname)
        .with_env("SQL_DRIVER", "postgresql+asyncpg")
        .with_env("S3_ENDPOINT", "minio:9000")
        .with_env("S3_ACCESS_KEY", "minioadmin")
        .with_env("S3_SECRET_KEY", "minioadmin")
        .with_env("S3_SECURE", "false")
        # Parent-page doc store bucket — the lifespan ensure_bucket creates it
        # in the MinIO testcontainer at startup.
        .with_env("PAGE_BUCKET", "parent-pages")
        .with_env("DEBUG", "true")
        .with_exposed_ports(_APP_PORT)
    )
    if vllm_container is not None:
        container.with_env(
            "COLBERT_HOST_URL", f"http://colbert:{VLLMContainer.HTTP_PORT}"
        )
        container.with_env("COLBERT_MODEL_NAME", _COLBERT_MODEL)
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


@pytest.fixture
def qdrant_client(qdrant_container: QdrantContainer) -> AsyncQdrantClient:
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))
    return AsyncQdrantClient(host=host, port=port, prefer_grpc=False)
