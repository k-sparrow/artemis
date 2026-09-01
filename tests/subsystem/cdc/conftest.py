"""Subsystem fixtures for the CDC pipeline end-to-end verification.

Stack: Network → Kafka (KRaft) → Postgres (postgres:16-alpine, wal_level=logical)
       → artemis-db-migrations → ksqlDB → ksqldb-init sidecar
       → Kafka Connect (artemis/cp-kafka-connect:latest)
       → two connectors (Debezium source + 1 JDBC sink)

Test actions insert/update rows directly into ingestion_status. Debezium reads
the WAL, publishes to Kafka. ksqlDB transforms and fans out to one output topic.
The JDBC sink upserts the rows into ingested_objects. Task-state visibility
(ingestion_tasks) was retired in Epic 22 — the storage service now reads
ingestion_status directly instead, so there's no second sink/table to verify
here.

Prerequisites:
    bazel run //tools/oci/images:kafka-connect.tarball
    bazel run //src/backend/alembic:tarball.dev
    bazel run //tools/oci/images/ksqldb:artemis_init_tarball
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer

from sqlalchemy.orm import Session

from src.backend.storage.api.models import Namespace, NamespaceType, Owner
from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer
from tests.lib.testcontainers.kafka import KSQLDbContainer, SchemaRegistryContainer

_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_PG_ALIAS = "postgres"
_PG_PORT = 5432
_PG_DB = "documents"
_PG_USER = "postgres"
_PG_PASS = "testpass"

_KAFKA_ALIAS = "broker"
_KSQLDB_ALIAS = "ksqldb-server"
_SR_ALIAS = "schema-registry"
_INIT_IMAGE = "artemis/ksqldb-init:latest"

_CONNECTOR_TIMEOUT_S = 90

_SOURCE_CONNECTOR = "DebeziumPostgresSourceConnector__CeleryResultBackendPublish"
_SINK_OBJECTS = "DebeziumJdbcSinkConnector__CeleryResultToIngestedObjects"


# ---------------------------------------------------------------------------
# _NoWait — skip testcontainers readiness check for one-shot sidecar containers
# ---------------------------------------------------------------------------


class _NoWait:
    def wait_until_ready(self, _container: DockerContainer) -> None:
        pass


# ---------------------------------------------------------------------------
# Network (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def network() -> Iterator[Network]:
    with Network() as n:
        yield n


# ---------------------------------------------------------------------------
# Kafka (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka(network: Network, request: pytest.FixtureRequest) -> KafkaContainer:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(network)
        .with_network_aliases(_KAFKA_ALIAS)
        .with_kwargs(hostname=_KAFKA_ALIAS)
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def bootstrap_server(kafka: KafkaContainer) -> str:
    return kafka.get_bootstrap_server()


# ---------------------------------------------------------------------------
# Schema Registry (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def schema_registry(
    network: Network, kafka: KafkaContainer, request: pytest.FixtureRequest
) -> SchemaRegistryContainer:
    container = (
        SchemaRegistryContainer(bootstrap_servers=kafka.get_internal_bootstrap_server())
        .with_network(network)
        .with_network_aliases(_SR_ALIAS)
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# Postgres (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres(network: Network, request: pytest.FixtureRequest) -> PostgresContainer:
    container = (
        PostgresContainer(
            image="postgres:16-alpine",
            username=_PG_USER,
            password=_PG_PASS,
            dbname=_PG_DB,
        )
        .with_network(network)
        .with_network_aliases(_PG_ALIAS)
        .with_kwargs(hostname=_PG_ALIAS)
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
def migrations_container(network: Network, postgres: PostgresContainer) -> None:
    from tests.lib.testcontainers.migrations import run_migrations_on_network

    run_migrations_on_network(
        network=network,
        username=postgres.username,
        password=postgres.password,
        dbname=postgres.dbname,
    )


@pytest.fixture(scope="session")
def postgres_engine(
    postgres: PostgresContainer, migrations_container: None
) -> Iterator[sa.Engine]:
    host = postgres.get_container_host_ip()
    port = postgres.get_exposed_port(_PG_PORT)
    engine = sa.create_engine(
        f"postgresql+psycopg://{_PG_USER}:{_PG_PASS}@{host}:{port}/{_PG_DB}"
    )
    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# ksqlDB + init sidecar (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ksqldb(
    network: Network,
    kafka: KafkaContainer,
    schema_registry: SchemaRegistryContainer,
    request: pytest.FixtureRequest,
) -> KSQLDbContainer:
    container = (
        KSQLDbContainer(bootstrap_servers=kafka.get_internal_bootstrap_server())
        .with_network(network)
        .with_network_aliases(_KSQLDB_ALIAS)
        .with_schema_registry(schema_registry.get_internal_url())
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def streams(
    network: Network,
    ksqldb: KSQLDbContainer,
    request: pytest.FixtureRequest,
) -> None:
    """Run the ksqldb-init sidecar to create all six streams (S3 + CDC).

    The sidecar runs ``ksql --file``, which PRINTS per-statement errors but still
    exits 0 — so a malformed stream/fan-out is silently skipped and the container
    exits cleanly. We therefore ALWAYS capture the init output and dump it on any
    test failure, so a swallowed ksqlDB error (e.g. a fan-out that never registered)
    is visible in the test log instead of needing a manual re-run to diagnose.
    """
    internal_url = f"http://{_KSQLDB_ALIAS}:{KSQLDbContainer.HTTP_PORT}"
    container = (
        DockerContainer(_INIT_IMAGE)
        .with_network(network)
        .with_env("KSQLDB_SERVER_URL", internal_url)
        .waiting_for(_NoWait())
    )
    container.start()
    result = container.get_wrapped_container().wait(timeout=120)
    stdout, stderr = container.get_logs()
    logs = f"stdout:\n{stdout.decode()}\nstderr:\n{stderr.decode()}"
    container.stop()

    if result["StatusCode"] != 0:
        raise RuntimeError(
            f"artemis/ksqldb-init exited with {result['StatusCode']}\n{logs}"
        )

    def _dump_init_logs_on_failure() -> None:
        if request.session.testsfailed:
            print(
                "\n=== artemis/ksqldb-init logs (ksql --file output) ===\n" + logs,
                flush=True,
            )

    request.addfinalizer(_dump_init_logs_on_failure)


# ---------------------------------------------------------------------------
# Kafka Connect (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_connect(
    network: Network,
    kafka: KafkaContainer,
    postgres: PostgresContainer,
    migrations_container: None,
    request: pytest.FixtureRequest,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(network)
        .with_network_aliases("kafka-connect")
        .with_log_level("WARN")
    )
    container.start()

    def _dump_logs_on_failure() -> None:
        # The Debezium source + JDBC sink connectors run in THIS container; a sink
        # task that throws on a record (e.g. a FAILURE row) fails silently as far as
        # the test is concerned — the row just never lands. Surface the connect logs
        # on any test failure so the task's exception is visible in the test log.
        if request.session.testsfailed:
            stdout, stderr = container.get_logs()
            print(
                "\n=== kafka-connect logs ===\n"
                + stdout.decode(errors="replace")
                + "\n--- stderr ---\n"
                + stderr.decode(errors="replace"),
                flush=True,
            )
        container.stop()

    request.addfinalizer(_dump_logs_on_failure)
    return container


@pytest.fixture(scope="session")
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()


# ---------------------------------------------------------------------------
# Connector deployment helpers
# ---------------------------------------------------------------------------


def _deploy_connector(url: str, name: str, config: dict) -> None:
    import httpx
    from fastapi import status

    resp = httpx.post(
        f"{url}/connectors", json={"name": name, "config": config}, timeout=10.0
    )
    if resp.status_code == status.HTTP_409_CONFLICT:
        return
    resp.raise_for_status()


def _wait_connector_running(
    url: str, name: str, timeout: int = _CONNECTOR_TIMEOUT_S
) -> None:
    import httpx
    from fastapi import status

    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        resp = httpx.get(f"{url}/connectors/{name}/status", timeout=5.0)
        if resp.status_code == status.HTTP_404_NOT_FOUND:
            time.sleep(2)
            continue
        resp.raise_for_status()
        last = resp.json()
        tasks = last.get("tasks", [])
        if (
            last.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t["state"] == "RUNNING" for t in tasks)
        ):
            return
        time.sleep(3)
    pytest.fail(
        f"Connector {name!r} did not reach RUNNING within {timeout}s. Last: {last}"
    )


# ---------------------------------------------------------------------------
# All connectors deployed and RUNNING (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def connectors(
    kafka_connect_url: str,
    streams: None,
    schema_registry: SchemaRegistryContainer,
) -> None:
    """Deploy the CDC connectors and wait for each to reach RUNNING."""
    sr_url = schema_registry.get_internal_url()

    _deploy_connector(
        kafka_connect_url,
        _SOURCE_CONNECTOR,
        {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "key.converter.schemas.enable": "false",
            "topic.prefix": "apollo.ingestion.celery.results",
            "table.include.list": "public.ingestion_status",
            "database.user": "debezium",
            "database.password": "debezium",
            "database.hostname": _PG_ALIAS,
            "database.port": str(_PG_PORT),
            "database.dbname": _PG_DB,
            "slot.name": "ingestion_status_rep_slot",
            "publication.name": "ingestion_status_publication",
            "publication.autocreate.mode": "disabled",
            "plugin.name": "pgoutput",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
        },
    )
    _deploy_connector(
        kafka_connect_url,
        _SINK_OBJECTS,
        {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": "artemis.celery.ingested_objects",
            "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
            "value.converter.schema.registry.url": sr_url,
            "value.converter.schemas.enable": "true",
            "key.converter": "io.confluent.connect.json.JsonSchemaConverter",
            "key.converter.schema.registry.url": sr_url,
            "key.converter.schemas.enable": "true",
            "connection.username": _PG_USER,
            "connection.password": _PG_PASS,
            "connection.url": f"jdbc:postgresql://{_PG_ALIAS}:{_PG_PORT}/{_PG_DB}",
            "table.name.format": "ingested_objects",
            "primary.key.mode": "record_key",
            "primary.key.fields": "id",
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "schema.evolution": "none",
            "use.time.zone": "UTC",
        },
    )
    for name in (_SOURCE_CONNECTOR, _SINK_OBJECTS):
        _wait_connector_running(kafka_connect_url, name)


# ---------------------------------------------------------------------------
# Per-test: namespace row + DB cleanup
# ---------------------------------------------------------------------------


@pytest.fixture
def namespace_row(postgres_engine: sa.Engine) -> uuid.UUID:
    """Insert an owner + namespace row and return the namespace_id."""
    owner_id = uuid.uuid4()
    namespace_id = uuid.uuid4()
    with Session(postgres_engine) as session:
        session.add(Owner(id=owner_id))
        session.add(
            Namespace(
                id=namespace_id,
                name=f"test-ns-{namespace_id}",
                type=NamespaceType.PRIVATE,
                owner_id=owner_id,
            )
        )
        session.commit()
    return namespace_id


@pytest.fixture(autouse=True)
def clean_db(postgres_engine: sa.Engine) -> Iterator[None]:
    yield
    with postgres_engine.begin() as conn:
        # Keep owner + namespace alive: the session-scoped JDBC sink connectors may
        # replay earlier messages after a task restart, and those messages reference
        # namespace_ids from previous tests. Truncating namespace would cause FK
        # violations on replay. Only clean the sink output tables and the CDC source.
        conn.execute(sa.text("TRUNCATE ingested_objects"))
        conn.execute(sa.text("DELETE FROM ingestion_status"))
