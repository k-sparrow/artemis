"""Container integration fixtures for the data_sources control plane.

Starts a full stack of real Docker containers on a shared bridge network:
  - KafkaContainer (KRaft) — broker
  - KafkaConnectContainer (artemis/cp-kafka-connect:latest) — Camel FileSource
  - PostgresContainer — data_source table
  - WireMockContainer — stubs the storage service
  - DockerContainer (artemis/backend-data-sources:dev) — system under test

The data_sources service container must be pre-built before running:
    bazel run //src/backend/enterprise/data_sources/api:tarball.dev

Session-scoped: Kafka broker, Postgres, WireMock, data_sources service.
Function-scoped: KafkaConnect (volume mounts must be set before container
start, so it is restarted per test with the test's watch_dir mounted RO).

Kafka consumer isolation
------------------------
The topic is pre-created once at session start. Each test's kafka_consumer
uses auto_offset_reset="latest" + warm-up poll() to pin to the current end
before any test action runs.

Per-test cleanup truncates the data_source table.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterator

import httpx
import pytest
import sqlalchemy as sa
from fastapi import status
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer
from wiremock.testing.testcontainer import WireMockContainer

from tests.lib.testcontainers.kafka import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_FILESYSTEM_TOPIC = "artemis.datasource.filesystem"
_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_DS_IMAGE = "artemis/backend-data-sources:dev"
_DS_PORT = 9500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Docker network (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


# ---------------------------------------------------------------------------
# Kafka broker (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> KafkaContainer:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(docker_network)
        .with_network_aliases("broker")
        .with_kwargs(hostname="broker")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_topic(kafka_bootstrap_server: str) -> str:
    """Pre-create the topic so consumers can reliably pin to 'latest' offset."""
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(_FILESYSTEM_TOPIC, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()
    return _FILESYSTEM_TOPIC


# ---------------------------------------------------------------------------
# Postgres (session-scoped)
# ---------------------------------------------------------------------------


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
            dbname="data_sources_test",
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
# WireMock — stubs the storage service (session-scoped)
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
        "namespace-create.json",
        {
            "request": {"method": "POST", "url": "/namespaces"},
            "response": {
                "status": 201,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {
                    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "type": "shared",
                    "name": "smoke-ns",
                },
            },
        },
    )
    wm.with_mapping(
        "namespace-delete.json",
        {
            "request": {"method": "DELETE", "urlPattern": "/namespaces/.*"},
            "response": {"status": 202},
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


# ---------------------------------------------------------------------------
# Data sources service container (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def data_sources_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    postgres_container: PostgresContainer,
    wiremock: WireMockContainer,
    migrations_container: None,
) -> DockerContainer:
    wiremock_internal_url = f"http://wiremock:{wiremock.http_server_port}"
    container = (
        DockerContainer(_DS_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("data-sources")
        .with_exposed_ports(_DS_PORT)
        .with_env(
            "SQL_DB_URL",
            f"postgresql+asyncpg://{postgres_container.username}:{postgres_container.password}"  # noqa: E501
            f"@postgres:5432/{postgres_container.dbname}",
        )
        .with_env(
            "KAFKA_CONNECT_URL",
            # KafkaConnect is function-scoped so we can't wire it here;
            # individual tests override this via the data_sources REST API.
            # The service only calls KafkaConnect on POST/DELETE — not at startup.
            "http://kafka-connect:8083",
        )
        .with_env("STORAGE_SERVICE_URL", wiremock_internal_url)
        .with_env("DEBUG", "true")
        # Production default is 10 minutes (see templates.py); tests need
        # near-real-time delivery within their own timeout budgets.
        .with_env("FILESOURCE_POLL_DELAY_MS", "2000")
    )
    container.start()
    request.addfinalizer(container.stop)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(_DS_PORT)
    try:
        _wait_for_http(f"http://{host}:{port}", "/health/liveness")
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== data-sources stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== data-sources stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container


@pytest.fixture(scope="session")
def data_sources_base_url(data_sources_container: DockerContainer) -> str:
    host = data_sources_container.get_container_host_ip()
    port = data_sources_container.get_exposed_port(_DS_PORT)
    return f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# Function-scoped KafkaConnect + watch_dir (restarted per test)
# ---------------------------------------------------------------------------


@pytest.fixture
def watch_dir(request: pytest.FixtureRequest) -> Iterator[Path]:
    """Temporary directory with pre-seeded files.

    Uses tempfile.mkdtemp() rather than pytest's tmp_path because Bazel's
    linux-sandbox remounts /tmp as a private tmpfs — paths under it are
    invisible to Docker volume mounts, which resolve against the real host
    filesystem. mkdtemp() writes to the real /tmp which Docker can mount.
    """
    d = Path(tempfile.mkdtemp(prefix="ds-test-watch-"))
    (d / "doc1.txt").write_text("first document")
    (d / "doc2.txt").write_text("second document")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    watch_dir: Path,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(docker_network)
        .with_network_aliases("kafka-connect")
        .with_volume_mapping(str(watch_dir), "/watch", "ro")
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()


# ---------------------------------------------------------------------------
# HTTP client pointing at the data_sources service
# ---------------------------------------------------------------------------


@pytest.fixture
def client(data_sources_base_url: str) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=data_sources_base_url, timeout=10.0) as c:
        yield c


@pytest.fixture(autouse=True)
def dump_ds_logs_on_failure(
    request: pytest.FixtureRequest,
    data_sources_container: DockerContainer,
    data_sources_base_url: str,
) -> Iterator[None]:
    yield
    if getattr(getattr(request.node, "rep_call", None), "failed", False):
        # Print the 502 response body so we can see the actual error detail
        try:
            resp = httpx.get(f"{data_sources_base_url}/data-sources", timeout=5.0)
            print(f"\n=== GET /data-sources: {resp.status_code} {resp.text} ===")
        except Exception as e:
            print(f"\n=== GET /data-sources failed: {e} ===")
        stdout, stderr = data_sources_container.get_logs()
        print("\n=== data-sources stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== data-sources stderr ===\n", stderr.decode(errors="replace"))


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)


# ---------------------------------------------------------------------------
# Kafka consumer (function-scoped, latest offset + warm-up)
# ---------------------------------------------------------------------------


@pytest.fixture
def kafka_consumer(
    kafka_bootstrap_server: str, kafka_topic: str
) -> Iterator[KafkaConsumer]:
    """Consumer pinned to the current end of the topic.

    Uses assign()+end_offsets()+seek() rather than subscribe()+auto_offset_reset
    or seek_to_end() because both of those are lazy: auto_offset_reset is
    evaluated at partition-assignment time (races with slow brokers) and
    seek_to_end() resolves on the next poll() inside list() (races with fast
    producers).  end_offsets() is a synchronous broker round-trip that returns
    the real HWM now, so seek() pins us before any test action fires.
    """
    tp = TopicPartition(kafka_topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=30_000,
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=1000)  # establish connection and fetch metadata
    hwm = consumer.end_offsets([tp])[tp]
    consumer.seek(tp, hwm)
    yield consumer
    consumer.close()


# ---------------------------------------------------------------------------
# Per-test cleanup
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_db(postgres_container: PostgresContainer) -> Iterator[None]:
    yield
    url = (
        postgres_container.get_connection_url()
        .replace("postgresql+psycopg2://", "postgresql+psycopg://")
        .replace("postgresql://", "postgresql+psycopg://")
    )
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE data_source"))
    engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_connector_state(
    client: httpx.Client,
    source_id: str,
    target_state: str = "RUNNING",
    timeout: int = 60,
) -> dict:
    """Poll GET /data-sources/{id} until kafka_status.state == target_state."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/data-sources/{source_id}")
        assert resp.status_code == status.HTTP_200_OK
        body = resp.json()
        if body.get("kafka_status") and body["kafka_status"]["state"] == target_state:
            return body
        time.sleep(2)
    raise TimeoutError(
        f"Connector {source_id} did not reach {target_state} within {timeout}s"
    )
