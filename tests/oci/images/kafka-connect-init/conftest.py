# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Fixtures for artemis/cp-kafka-connect-init CDC connector tests.

Stack: Kafka + artemis/postgres:latest + artemis/cp-kafka-connect:latest.

The postgres image ships init.sql which creates:
  - debezium user with REPLICATION privilege
  - apollo_celery_taskmeta table + celery_results_publication
  - apollo_taskmeta_table_rep_slot replication slot (created by Debezium on first connect)

The storage service tables (ingested_objects, ingestion_tasks) are not in
init.sql — they are created here via SQLAlchemy using the storage service
models so the JDBC sink connectors can verify their target tables exist.

Prerequisites:
    bazel run //tools/oci/images:kafka-connect.tarball
    bazel run //tools/oci/images/postgres:tarball
"""

from __future__ import annotations

import time

import httpx
import pytest
import sqlalchemy as sa
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from src.backend.storage.api.models import Base
from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_PG_IMAGE = "artemis/postgres:latest"
_PG_HOST_ALIAS = "postgres"
_PG_PORT = 5432
_PG_DB = "documents"
_PG_SUPERUSER = "postgres"
_PG_SUPERPASS = "testpass"

_SINK_TOPICS = [
    "artemis.celery.ingested_objects",
    "artemis.celery.ingestion_tasks",
]


def _wait_for_http(url: str, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if (
                httpx.get(url, timeout=3.0).status_code
                < httpx.codes.INTERNAL_SERVER_ERROR
            ):
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"{url} did not become ready within {timeout}s")


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


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
def sink_topics(kafka_bootstrap_server: str) -> None:
    """Pre-create output topics that the JDBC sink connectors consume from."""
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(t, num_partitions=1, replication_factor=1) for t in _SINK_TOPICS]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()


@pytest.fixture(scope="session")
def postgres_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> DockerContainer:
    """artemis/postgres:latest with wal_level=logical and standard dev credentials.

    Runs init.sql which creates:
      - debezium user + REPLICATION + apollo_celery_taskmeta + celery_results_publication
    """
    container = (
        DockerContainer(_PG_IMAGE)
        .with_network(docker_network)
        .with_network_aliases(_PG_HOST_ALIAS)
        .with_kwargs(hostname=_PG_HOST_ALIAS)
        .with_exposed_ports(_PG_PORT)
        .with_env("POSTGRES_USER", _PG_SUPERUSER)
        .with_env("POSTGRES_PASSWORD", _PG_SUPERPASS)
        .with_env("POSTGRES_DB", _PG_DB)
        .with_command(
            "postgres"
            " -c wal_level=logical"
            " -c max_wal_senders=10"
            " -c max_replication_slots=10"
        )
    )
    container.start()
    request.addfinalizer(container.stop)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_PG_PORT)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            engine = sa.create_engine(
                f"postgresql+psycopg://{_PG_SUPERUSER}:{_PG_SUPERPASS}"
                f"@{host}:{port}/{_PG_DB}"
            )
            with engine.connect():
                pass
            engine.dispose()
            break
        except Exception:
            time.sleep(2)
    else:
        raise TimeoutError("Postgres did not become ready within 60s")

    return container


@pytest.fixture(scope="session")
def storage_tables(postgres_container: DockerContainer) -> None:
    """Create ingested_objects and ingestion_tasks (and their FK anchors) via SQLAlchemy.

    These tables are not in init.sql — the storage service normally creates them
    via create_all() on startup. We replicate that here so the JDBC sink connectors
    can connect and verify the target table exists.
    """
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(_PG_PORT)
    url = (
        f"postgresql+psycopg://{_PG_SUPERUSER}:{_PG_SUPERPASS}"
        f"@{host}:{port}/{_PG_DB}"
    )
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)
    engine.dispose()


@pytest.fixture(scope="session")
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    postgres_container: DockerContainer,
    storage_tables: None,
    sink_topics: None,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(docker_network)
        .with_network_aliases("kafka-connect")
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()
