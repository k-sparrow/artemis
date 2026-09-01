"""Tier 3 container integration fixtures for the storage service.

Starts a full stack of real Docker containers on a shared bridge network:
  - Kafka (confluentinc/cp-kafka via KafkaContainer, KRaft mode)
  - MinIO (with Kafka notification configured)
  - Postgres
  - Storage service (artemis/backend-storage:dev) — system under test

The storage service container must be pre-built before running these tests:
    bazel run //src/backend/storage/api:tarball.dev

All containers are session-scoped (started once).

Kafka consumer isolation
------------------------
The Kafka topic is pre-created once at session start. Each test's
kafka_consumer fixture uses auto_offset_reset="latest" and does a warm-up
poll() before yielding. This pins the consumer's read position to the current
end of the topic before any test action produces messages, so each consumer
only sees messages produced by its own test — no cross-test contamination.

Per-test cleanup truncates DB tables and removes MinIO objects.

Kafka listeners
---------------
KafkaContainer sets up two advertised listeners:
  - PLAINTEXT://{host}:{port}  — reachable from the test process
  - BROKER://{first_container_ip}:9092  — reachable from other containers
                                          (MinIO connects via 'kafka' alias, gets
                                           redirected to this IP via broker metadata)
"""

from __future__ import annotations

import time
from typing import Iterator

import httpx
import pytest
import sqlalchemy as sa
from fastapi import status
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from minio import Minio
from minio.notificationconfig import NotificationConfig, QueueConfig
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from tests.lib.testcontainers.kafka.kafka import KafkaContainer
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

_BUCKET = "artemis"
_KAFKA_EVENT = "ARTEMIS"
_KAFKA_TOPIC = "artemis.ingestion.storage.s3"
_STORAGE_PORT = 7000


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
# Kafka (session-scoped)
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
        .with_network_aliases("kafka")
        .with_kwargs(hostname="kafka")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_bootstrap_server(kafka_container: KafkaContainer) -> str:
    """Host-side bootstrap server for test consumers."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_topic(kafka_bootstrap_server: str) -> str:
    """Pre-create the Kafka topic once so consumers can pin to 'latest' offset.

    Without pre-creation, the topic is auto-created by the first produced
    message. A consumer using auto_offset_reset='latest' that subscribes
    before the topic exists gets assigned when the first message arrives —
    but by then 'latest' points past that message and the consumer misses it.
    """
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(_KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()
    return _KAFKA_TOPIC


# ---------------------------------------------------------------------------
# MinIO with Kafka notification (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def minio_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
) -> MinioContainer:
    container = (
        MinioContainer(image="quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z")
        .with_network(docker_network)
        .with_network_aliases("minio")
        # Newer MinIO uses ROOT_USER/ROOT_PASSWORD; set both for compatibility.
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_env(f"MINIO_NOTIFY_KAFKA_ENABLE_{_KAFKA_EVENT}", "on")
        .with_env(f"MINIO_NOTIFY_KAFKA_BROKERS_{_KAFKA_EVENT}", "kafka:9092")
        .with_env(f"MINIO_NOTIFY_KAFKA_TOPIC_{_KAFKA_EVENT}", _KAFKA_TOPIC)
    )
    container.start()
    request.addfinalizer(container.stop)
    client = container.get_client()
    if not client.bucket_exists(_BUCKET):
        client.make_bucket(_BUCKET)
    # Attach the S3 event notification rule to the bucket.
    # The env vars above enable the Kafka *target*; this rule tells MinIO
    # which events on which bucket to forward to that target.
    client.set_bucket_notification(
        _BUCKET,
        NotificationConfig(
            queue_config_list=[
                QueueConfig(
                    f"arn:minio:sqs::{_KAFKA_EVENT}:kafka",
                    ["s3:ObjectCreated:*"],
                    config_id="artemis-kafka",
                )
            ]
        ),
    )
    return container


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
            dbname="storage_test",
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
# Storage service container (session-scoped)
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


@pytest.fixture(scope="session")
def storage_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    minio_container: MinioContainer,
    postgres_container: PostgresContainer,
    migrations_container: None,
) -> DockerContainer:
    container = (
        DockerContainer("artemis/backend-storage:dev")
        .with_network(docker_network)
        .with_network_aliases("storage")
        .with_exposed_ports(_STORAGE_PORT)
        .with_env(
            "SQL_DB_URL",
            f"postgresql+asyncpg://{postgres_container.username}:{postgres_container.password}"  # noqa: E501
            f"@postgres:5432/{postgres_container.dbname}",
        )
        .with_env("S3_ENDPOINT_URL", "minio:9000")
        .with_env("S3_ACCESS_KEY", "minioadmin")
        .with_env("S3_SECRET_KEY", "minioadmin")
        .with_env("S3_ARTEMIS_BUCKET", _BUCKET)
        .with_env("S3_ARTEMIS_BUCKET_KAFKA_EVENT", _KAFKA_EVENT)
        .with_env("DEBUG", "true")
    )
    container.start()

    def dump_logs_and_stop():
        stdout, stderr = container.get_logs()
        print("\n=== storage container stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== storage container stderr ===\n", stderr.decode(errors="replace"))
        container.stop()

    request.addfinalizer(dump_logs_and_stop)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_STORAGE_PORT)
    try:
        _wait_for_http(f"http://{host}:{port}", "/health/readiness")
    except TimeoutError:
        raise
    return container


@pytest.fixture(scope="session")
def storage_base_url(storage_container: DockerContainer) -> str:
    host = storage_container.get_container_host_ip()
    port = storage_container.get_exposed_port(_STORAGE_PORT)
    return f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# Test clients (function-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture
def client(storage_base_url: str) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=storage_base_url, timeout=10.0) as c:
        yield c


@pytest.fixture(scope="session")
def test_minio_client(minio_container: MinioContainer) -> Minio:
    return minio_container.get_client()


@pytest.fixture
def kafka_consumer(
    kafka_bootstrap_server: str, kafka_topic: str
) -> Iterator[KafkaConsumer]:
    """A consumer pinned to the current end of the topic.

    Uses assign()+seek_to_end() rather than subscribe()+auto_offset_reset
    because auto_offset_reset is unreliable: MinIO emits S3 events
    asynchronously, so a message from a previous test can land in Kafka after
    this consumer is created, causing the 'latest' pin to land before it and
    the stale message to bleed into this test.  seek_to_end() resolves the
    actual high-water mark and positions the consumer exactly there.
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
    # seek_to_end() is lazy — it resolves on the NEXT poll() inside list().
    # If the test action produces a message before that poll() runs, the HWM
    # resolves past it and the message is missed.  end_offsets() is a
    # synchronous broker round-trip that returns the real HWM now, so seek()
    # pins us to exactly the right offset before any test action fires.
    hwm = consumer.end_offsets([tp])[tp]
    consumer.seek(tp, hwm)
    yield consumer
    consumer.close()


# ---------------------------------------------------------------------------
# Per-test cleanup (autouse)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_db(postgres_container: PostgresContainer) -> Iterator[None]:
    yield
    # testcontainers returns a postgresql:// URL; psycopg3 needs the +psycopg dialect.
    url = (
        postgres_container.get_connection_url()
        .replace("postgresql+psycopg2://", "postgresql+psycopg://")
        .replace("postgresql://", "postgresql+psycopg://")
    )
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE owner, namespace, ingested_objects CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def clean_minio(test_minio_client: Minio) -> Iterator[None]:
    yield
    for obj in test_minio_client.list_objects(_BUCKET, recursive=True):
        test_minio_client.remove_object(_BUCKET, obj.object_name)
