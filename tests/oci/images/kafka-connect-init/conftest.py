# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Fixtures for artemis/cp-kafka-connect-init CDC connector tests.

Stack:
   Kafka +
   postgres:16-alpine +
   artemis-db-migrations +
   artemis/cp-kafka-connect:latest.

Alembic migrations create:
  - debezium user with REPLICATION privilege
  - ingestion_status table + ingestion_status_publication
  - all storage service tables (ingested_objects, owner, namespace)

Prerequisites:
    bazel run //tools/oci/images:kafka-connect.tarball
    bazel run //src/backend/alembic:tarball.dev
"""

from __future__ import annotations

import time

import httpx
import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_PG_HOST_ALIAS = "postgres"
_PG_PORT = 5432
_PG_DB = "documents"
_PG_SUPERUSER = "postgres"
_PG_SUPERPASS = "testpass"

_SINK_TOPICS = [
    "artemis.celery.ingested_objects",
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
) -> PostgresContainer:
    container = (
        PostgresContainer(
            image="postgres:16-alpine",
            username=_PG_SUPERUSER,
            password=_PG_SUPERPASS,
            dbname=_PG_DB,
        )
        .with_network(docker_network)
        .with_network_aliases(_PG_HOST_ALIAS)
        .with_kwargs(hostname=_PG_HOST_ALIAS)
        .with_command(
            "postgres"
            " -c wal_level=logical"
            " -c max_wal_senders=10"
            " -c max_replication_slots=10"
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
        network=docker_network,
        username=postgres_container.username,
        password=postgres_container.password,
        dbname=postgres_container.dbname,
    )


@pytest.fixture(scope="session")
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    postgres_container: PostgresContainer,
    migrations_container: None,
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
