# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""CDC pipeline connector deployment tests.

Verifies that the three connectors deployed by artemis/cp-kafka-connect-init
can be deployed against a plain Postgres (postgres:16-alpine + Alembic migrations)
and reach the RUNNING state within a reasonable timeout.

Connectors under test
---------------------
1. DebeziumPostgresSourceConnector__CeleryResultBackendPublish
   Reads apollo_celery_taskmeta WAL via pgoutput; requires wal_level=logical,
   celery_results_publication, and the debezium replication user.

2. DebeziumJdbcSinkConnector__CeleryResultToIngestedObjects
   Upserts rows into ingested_objects from artemis.celery.ingested_objects.

3. DebeziumJdbcSinkConnector__CeleryResultToIngestionTasks
   Upserts rows into ingestion_tasks from artemis.celery.ingestion_tasks.

Prerequisites:
    bazel run //tools/oci/images:kafka-connect.tarball
    bazel run //src/backend/alembic:tarball.dev
"""

from __future__ import annotations

import time

import httpx
import pytest
from fastapi import status

_CONNECTOR_RUNNING_TIMEOUT_S = 90

_PG_HOST = "postgres"
_PG_PORT = 5432
_PG_DB = "documents"


def _deploy_connector(kafka_connect_url: str, name: str, config: dict) -> None:
    resp = httpx.post(
        f"{kafka_connect_url}/connectors",
        json={"name": name, "config": config},
        timeout=10.0,
    )
    if resp.status_code == status.HTTP_409_CONFLICT:
        return
    resp.raise_for_status()


def _wait_connector_running(kafka_connect_url: str, name: str) -> None:
    deadline = time.monotonic() + _CONNECTOR_RUNNING_TIMEOUT_S
    last: dict = {}
    while time.monotonic() < deadline:
        resp = httpx.get(f"{kafka_connect_url}/connectors/{name}/status", timeout=5.0)
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
        f"Connector {name!r} did not reach RUNNING "
        f"within {_CONNECTOR_RUNNING_TIMEOUT_S}s. "
        f"Last status: {last}"
    )


def _delete_connector(kafka_connect_url: str, name: str) -> None:
    httpx.delete(f"{kafka_connect_url}/connectors/{name}", timeout=5.0)


@pytest.mark.integration
class TestDebeziumSourceConnector:
    """
    Debezium PostgreSQL source connector reaches RUNNING against a migrated Postgres.
    """

    _NAME = "DebeziumPostgresSourceConnector__CeleryResultBackendPublish"

    @pytest.fixture(autouse=True)
    def connector(self, kafka_connect_url: str) -> None:
        _deploy_connector(
            kafka_connect_url,
            self._NAME,
            {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
                "topic.prefix": "apollo.ingestion.celery.results",
                "table.include.list": "public.apollo_celery_taskmeta",
                "database.user": "debezium",
                "database.password": "debezium",
                "database.hostname": _PG_HOST,
                "database.port": str(_PG_PORT),
                "database.dbname": _PG_DB,
                "slot.name": "apollo_taskmeta_table_rep_slot",
                "publication.name": "celery_results_publication",
                "publication.autocreate.mode": "disabled",
                "plugin.name": "pgoutput",
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
            },
        )
        yield
        _delete_connector(kafka_connect_url, self._NAME)

    def test_reaches_running_state(self, kafka_connect_url: str) -> None:
        _wait_connector_running(kafka_connect_url, self._NAME)


@pytest.mark.integration
class TestJdbcSinkIngestedObjects:
    """JDBC sink for ingested_objects reaches RUNNING with the target table present."""

    _NAME = "DebeziumJdbcSinkConnector__CeleryResultToIngestedObjects"

    @pytest.fixture(autouse=True)
    def connector(self, kafka_connect_url: str) -> None:
        _deploy_connector(
            kafka_connect_url,
            self._NAME,
            {
                "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "artemis.celery.ingested_objects",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "connection.username": "postgres",
                "connection.password": "testpass",
                "connection.url": f"jdbc:postgresql://{_PG_HOST}:{_PG_PORT}/{_PG_DB}",
                "table.name.format": "ingested_objects",
                "primary.key.mode": "record_value",
                "primary.key.fields": "id",
                "insert.mode": "upsert",
                "schema.evolution": "none",
                "use.time.zone": "UTC",
            },
        )
        yield
        _delete_connector(kafka_connect_url, self._NAME)

    def test_reaches_running_state(self, kafka_connect_url: str) -> None:
        _wait_connector_running(kafka_connect_url, self._NAME)


@pytest.mark.integration
class TestJdbcSinkIngestionTasks:
    """JDBC sink for ingestion_tasks reaches RUNNING with the target table present."""

    _NAME = "DebeziumJdbcSinkConnector__CeleryResultToIngestionTasks"

    @pytest.fixture(autouse=True)
    def connector(self, kafka_connect_url: str) -> None:
        _deploy_connector(
            kafka_connect_url,
            self._NAME,
            {
                "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "artemis.celery.ingestion_tasks",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "connection.username": "postgres",
                "connection.password": "testpass",
                "connection.url": f"jdbc:postgresql://{_PG_HOST}:{_PG_PORT}/{_PG_DB}",
                "table.name.format": "ingestion_tasks",
                "primary.key.mode": "record_value",
                "primary.key.fields": "task_id",
                "insert.mode": "upsert",
                "schema.evolution": "none",
                "use.time.zone": "UTC",
            },
        )
        yield
        _delete_connector(kafka_connect_url, self._NAME)

    def test_reaches_running_state(self, kafka_connect_url: str) -> None:
        _wait_connector_running(kafka_connect_url, self._NAME)
