"""Tier 4 end-to-end test fixtures.

Full-stack — no stubs.  Every service is the real Docker image:

  Infrastructure (exposed ports, reachable by all parties):
    - RabbitMQ    — Celery AMQP broker
    - Postgres    — Celery result backend + SQL record manager
    - MinIO       — S3 source bucket + parsed-chunk intermediate store
    - Qdrant      — vectorstore
    - Docling     — document-parsing back-end (CPU image)
    - TEI         — text-embedding inference (CPU image)

  Services (network_mode="host", bind fixed ports on the host):
    - Parsing service   port 10001  depends on Docling
    - Indexing service  port 10000  depends on Qdrant + TEI + Postgres
    - Worker                        depends on RabbitMQ + Postgres + MinIO
                                    + both services above

Services use ``network_mode="host"`` so they can reach every infrastructure
container via ``localhost:<host-mapped-port>``, matching the Tier 3 pattern.

Load all three service images before running:

  bazel run //src/backend/controller/worker:image.tarball
  bazel run //src/backend/indexing:image.tarball
  bazel run //src/backend/parsing:image.tarball

Run with:

  bazel test //tests/e2e/... --test_arg=--e2e
"""

from __future__ import annotations

import io
import time
import uuid
from pathlib import Path

import pytest
from minio import Minio
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.qdrant import QdrantContainer
from testcontainers.rabbitmq import RabbitMqContainer

from tests.lib.testcontainers.tei import TEIContainer

# ---------------------------------------------------------------------------
# Image tags
# ---------------------------------------------------------------------------

_DOCLING_IMAGE = "ghcr.io/docling-project/docling-serve-cu128:v1.15.0"
_PARSING_IMAGE = "artemis/backend-parsing:dev"
_INDEXING_IMAGE = "artemis/backend-indexing:dev"
_WORKER_IMAGE = "artemis/backend-controller-worker:latest"

_DOCLING_PORT = 5001
_PARSING_PORT = 10001
_INDEXING_PORT = 10000

# Every test in this session indexes into (and queries from) the same Qdrant
# collection; per-test isolation is achieved by using a unique namespace_id.
E2E_COLLECTION = "e2e_collection"

_EMBEDDING_MODEL = "sentence-transformers/msmarco-MiniLM-L-12-v3"


# ---------------------------------------------------------------------------
# Infrastructure containers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rabbitmq_container(request: pytest.FixtureRequest) -> RabbitMqContainer:
    container = RabbitMqContainer(image="rabbitmq:4.1.2-management-alpine")
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def postgres_container(request: pytest.FixtureRequest) -> PostgresContainer:
    container = PostgresContainer(
        image="postgres:16-alpine",
        username="artemis",
        password="artemis",
        dbname="artemis",
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def minio_container(request: pytest.FixtureRequest) -> MinioContainer:
    container = MinioContainer()
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def qdrant_container(request: pytest.FixtureRequest) -> QdrantContainer:
    container = QdrantContainer("qdrant/qdrant:v1.17")
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def docling_container(request: pytest.FixtureRequest) -> DockerContainer:
    container = (
        DockerContainer(_DOCLING_IMAGE)
        .with_exposed_ports(_DOCLING_PORT)
        .waiting_for(HttpWaitStrategy(_DOCLING_PORT, "/health").for_status_code(200))
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def tei_container(request: pytest.FixtureRequest) -> TEIContainer:
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = TEIContainer(model_id=_EMBEDDING_MODEL)
    if hf_cache.exists():
        container.with_hf_cache(str(hf_cache))
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# Service containers  (network_mode="host" — they see localhost infrastructure)
# ---------------------------------------------------------------------------


def _wait_for_liveness(url: str, timeout: int = 120) -> None:
    import httpx

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=3.0).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"Service at {url} did not become ready within {timeout}s")


@pytest.fixture(scope="session")
def parsing_container(
    docling_container: DockerContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    docling_host = docling_container.get_container_host_ip()
    docling_port = docling_container.get_exposed_port(_DOCLING_PORT)

    container = (
        DockerContainer(_PARSING_IMAGE)
        .with_kwargs(network_mode="host")
        .with_env("DOCLING_SERVE_URI", f"http://{docling_host}:{docling_port}")
    )
    container.start()
    request.addfinalizer(container.stop)
    _wait_for_liveness(f"http://localhost:{_PARSING_PORT}/health/liveness")
    return container


@pytest.fixture(scope="session")
def indexing_container(
    qdrant_container: QdrantContainer,
    tei_container: TEIContainer,
    postgres_container: PostgresContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    qdrant_host = qdrant_container.get_container_host_ip()
    qdrant_port = qdrant_container.get_exposed_port(6333)
    tei_url = tei_container.get_url()
    pg_host = postgres_container.get_container_host_ip()
    pg_port = postgres_container.get_exposed_port(5432)

    container = (
        DockerContainer(_INDEXING_IMAGE)
        .with_kwargs(network_mode="host")
        .with_env("QDRANT_HOST_URL", f"http://{qdrant_host}:{qdrant_port}")
        .with_env("QDRANT_COLLECTION_NAME", E2E_COLLECTION)
        .with_env("TEI_HOST_URL", tei_url)
        .with_env("SQL_DB_HOST", pg_host)
        .with_env("SQL_DB_PORT", str(pg_port))
        .with_env("SQL_DB_USER", postgres_container.username)
        .with_env("SQL_DB_PASSWORD", postgres_container.password)
        .with_env("SQL_DB_DATABASE", postgres_container.dbname)
        .with_env("SQL_DRIVER", "postgresql+asyncpg")
    )
    container.start()
    request.addfinalizer(container.stop)
    _wait_for_liveness(f"http://localhost:{_INDEXING_PORT}/health/liveness")
    return container


@pytest.fixture(scope="session")
def worker_container(
    rabbitmq_container: RabbitMqContainer,
    postgres_container: PostgresContainer,
    minio_container: MinioContainer,
    parsing_container: DockerContainer,
    indexing_container: DockerContainer,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    pg_host = postgres_container.get_container_host_ip()
    pg_port = postgres_container.get_exposed_port(5432)
    minio_host = minio_container.get_container_host_ip()
    minio_port = minio_container.get_exposed_port(9000)
    broker_url = f"amqp://guest:guest@{rabbit_host}:{rabbit_port}/"

    container = (
        DockerContainer(_WORKER_IMAGE)
        .with_kwargs(network_mode="host")
        .with_env("RABBITMQ_USER", "guest")
        .with_env("RABBITMQ_PASSWORD", "guest")
        .with_env("RABBITMQ_HOST", rabbit_host)
        .with_env("RABBITMQ_PORT", str(rabbit_port))
        .with_env("RABBITMQ_VHOST", "")
        .with_env("SQL_DB_HOST", pg_host)
        .with_env("SQL_DB_PORT", str(pg_port))
        .with_env("SQL_DB_USER", postgres_container.username)
        .with_env("SQL_DB_PASSWORD", postgres_container.password)
        .with_env("SQL_DB_DATABASE", postgres_container.dbname)
        .with_env("SQL_DRIVER", "postgresql+psycopg")
        .with_env("S3_ENDPOINT", f"{minio_host}:{minio_port}")
        .with_env("S3_ACCESS_KEY", "minioadmin")
        .with_env("S3_SECRET_KEY", "minioadmin")
        .with_env("S3_SECURE", "false")
        .with_env("PARSING_SERVICE_URL", f"http://localhost:{_PARSING_PORT}")
        .with_env("INGESTION_SERVICE_URL", f"http://localhost:{_INDEXING_PORT}")
        .with_env("EXCHANGE_NAME", "artemis")
        .with_env("PARSED_CHUNKS_BUCKET", "parsed-chunks")
        .with_env("HTTPX_TIMEOUT", "120.0")
    )
    container.start()

    try:
        _wait_for_worker(broker_url, timeout=60)
    except TimeoutError:
        logs = container.get_logs()
        container.stop()
        raise TimeoutError(
            f"Worker container did not become ready\n"
            f"broker_url={broker_url!r}\n"
            f"Worker logs:\n{logs}"
        ) from None

    def _stop_and_dump() -> None:
        stdout, stderr = container.get_logs()
        print(
            "\n=== Worker stdout ===\n"
            + stdout.decode(errors="replace")
            + "\n=== Worker stderr ===\n"
            + stderr.decode(errors="replace"),
            flush=True,
        )
        container.stop()

    request.addfinalizer(_stop_and_dump)
    return container


def _wait_for_worker(broker_url: str, timeout: int = 60) -> None:
    from celery import Celery as _Celery

    check = _Celery(broker=broker_url)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check.control.inspect(timeout=2).active_queues():
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("Celery worker did not register on broker within timeout")


# ---------------------------------------------------------------------------
# Test-side clients
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def dispatch_app(
    rabbitmq_container: RabbitMqContainer,
    worker_container: DockerContainer,
):
    from celery import Celery
    from kombu import Exchange, Queue

    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    broker_url = f"amqp://guest:guest@{rabbit_host}:{rabbit_port}/"

    _exchange = Exchange("artemis", type="direct")
    app = Celery(broker=broker_url)
    app.conf.task_serializer = "json"
    app.conf.accept_content = ["json"]
    app.conf.task_queues = [
        Queue(
            "artemis.ingestion.fetch-and-parse",
            exchange=_exchange,
            routing_key="fetch-and-parse",
            durable=True,
        ),
        Queue(
            "artemis.ingestion.index",
            exchange=_exchange,
            routing_key="index",
            durable=True,
        ),
    ]
    return app


@pytest.fixture(scope="session")
def minio_client(minio_container: MinioContainer) -> Minio:
    return minio_container.get_client()


@pytest.fixture(scope="session")
def qdrant_client(qdrant_container: QdrantContainer) -> AsyncQdrantClient:
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))
    return AsyncQdrantClient(host=host, port=port, prefer_grpc=False)


# ---------------------------------------------------------------------------
# Per-test helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def s3_source_bucket(minio_client: Minio):
    bucket = "ingest-source"
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
    yield bucket
    for obj in minio_client.list_objects(bucket, recursive=True):
        minio_client.remove_object(bucket, obj.object_name)


@pytest.fixture
def namespace_id() -> uuid.UUID:
    return uuid.uuid4()


# ---------------------------------------------------------------------------
# Dispatch helpers
# ---------------------------------------------------------------------------


def upload_file(
    minio_client: Minio,
    bucket: str,
    key: str,
    content: bytes = b"# Hello\n\nSome test content for e2e ingestion.\n",
) -> None:
    minio_client.put_object(
        bucket, key, io.BytesIO(content), len(content), content_type="text/markdown"
    )


def dispatch_ingest(
    dispatch_app,
    bucket: str,
    key: str,
    namespace_id: uuid.UUID,
    action: str = "CREATE",
) -> None:
    dispatch_app.send_task(
        "tasks.ingest",
        kwargs={
            "s3": {"bucket": bucket, "object": key, "size": 0},
            "source": {
                "source": key,
                "content_type": "text/markdown",
                "obj_id": str(uuid.uuid5(namespace_id, key)),
                "object_type": "file",
            },
            "upload_action": action,
            "info": {"namespace_id": str(namespace_id)},
        },
        queue="artemis.ingestion.fetch-and-parse",
        routing_key="fetch-and-parse",
    )


def dispatch_delete_namespace(dispatch_app, namespace_id: uuid.UUID) -> None:
    dispatch_app.send_task(
        "tasks.delete_namespace",
        kwargs={"namespace_id": str(namespace_id)},
        queue="artemis.ingestion.index",
        routing_key="index",
    )


# ---------------------------------------------------------------------------
# Qdrant polling helpers
# ---------------------------------------------------------------------------


async def wait_for_points(
    client: AsyncQdrantClient,
    collection: str,
    namespace_id: uuid.UUID,
    min_count: int = 1,
    timeout: int = 120,
) -> None:
    """Block until at least *min_count* vectors exist for *namespace_id*."""
    ns_filter = Filter(
        must=[
            FieldCondition(
                key="metadata.namespace",
                match=MatchValue(value=str(namespace_id)),
            )
        ]
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = await client.count(
                collection_name=collection,
                count_filter=ns_filter,
                exact=True,
            )
            if result.count >= min_count:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(
        f"Qdrant collection '{collection}' did not reach {min_count} point(s) "
        f"for namespace {namespace_id} within {timeout}s"
    )


async def wait_for_empty(
    client: AsyncQdrantClient,
    collection: str,
    namespace_id: uuid.UUID,
    timeout: int = 60,
) -> None:
    """Block until no vectors remain for *namespace_id*."""
    ns_filter = Filter(
        must=[
            FieldCondition(
                key="metadata.namespace",
                match=MatchValue(value=str(namespace_id)),
            )
        ]
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = await client.count(
                collection_name=collection,
                count_filter=ns_filter,
                exact=True,
            )
            if result.count == 0:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(
        f"Qdrant collection '{collection}' still has points for namespace "
        f"{namespace_id} after {timeout}s"
    )
