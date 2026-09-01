"""Subsystem tests: CDC pipeline writes expected rows to ingested_objects.

Verifies the complete path from a Celery result row appearing in Postgres through
Debezium WAL capture → Kafka → ksqlDB fan-out → JDBC sink → DB tables.

The contract tests (tests/contracts/cdc/) verify the Kafka message shape in
isolation. These tests verify that the full pipeline actually populates the DB.

All containers are session-scoped; only DB rows are cleaned between tests.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx
import pytest
import sqlalchemy as sa


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_POLL_TIMEOUT_S = 90
_POLL_INTERVAL_S = 2

_SOURCE_CONNECTOR = "DebeziumPostgresSourceConnector__CeleryResultBackendPublish"
_SINK_OBJECTS = "DebeziumJdbcSinkConnector__CeleryResultToIngestedObjects"


def _connector_diagnostics(kafka_connect_url: str) -> str:
    lines = []
    for name in (_SOURCE_CONNECTOR, _SINK_OBJECTS):
        try:
            resp = httpx.get(
                f"{kafka_connect_url}/connectors/{name}/status", timeout=5.0
            )
            lines.append(f"  {name}: {resp.json()}")
        except Exception as e:
            lines.append(f"  {name}: ERROR {e}")
    return "\n".join(lines)


def _insert_ingestion_status(
    engine: sa.Engine,
    *,
    task_id: uuid.UUID,
    obj_id: uuid.UUID,
    namespace_id: uuid.UUID,
    group_id: uuid.UUID | None = None,
    status: str = "success",
    stage: str = "tasks.index",
    updated_at: str = "2026-01-01T12:00:00",
    operation: str = "CREATE",
) -> None:
    """Insert a row into ingestion_status to trigger Debezium WAL capture.

    Real tasks INSERT once (ingest()) then UPDATE in place through the chain;
    these tests only care about the row Debezium ships downstream, so a single
    INSERT at the desired terminal stage/status is equivalent for CDC purposes.
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO ingestion_status"
                " (task_id, namespace_id, obj_id, source, object_type,"
                "  content_type, size_bytes, group_id, operation, stage,"
                "  status, failure_reason, updated_at)"
                " VALUES (:task_id, :namespace_id, :obj_id, :source, :object_type,"
                "         :content_type, :size_bytes, :group_id, :operation, :stage,"
                "         :status, NULL, CAST(:updated_at AS TIMESTAMP))"
            ),
            {
                "task_id": str(task_id),
                "namespace_id": str(namespace_id),
                "obj_id": str(obj_id),
                "source": "doc.txt",
                "object_type": "file",
                "content_type": "text/plain",
                "size_bytes": None if operation == "DELETE" else 2048,
                "group_id": str(group_id) if group_id else None,
                "operation": operation,
                "stage": stage,
                "status": status,
                "updated_at": updated_at,
            },
        )


def _poll_row(
    engine: sa.Engine,
    query: str,
    params: dict[str, Any],
    timeout: int = _POLL_TIMEOUT_S,
) -> Any | None:
    """Poll until a query returns a row. Returns None on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            row = conn.execute(sa.text(query), params).fetchone()
            if row is not None:
                return row
        time.sleep(_POLL_INTERVAL_S)
    return None


def _assert_row(
    row: Any | None,
    query: str,
    params: dict[str, Any],
    kafka_connect_url: str,
) -> Any:
    """Assert a row was found; include connector diagnostics on failure."""
    if row is None:
        pytest.fail(
            f"No row found within {_POLL_TIMEOUT_S}s.\n"
            f"Query: {query}\nParams: {params}\n"
            f"Connector statuses:\n{_connector_diagnostics(kafka_connect_url)}"
        )
    return row


# ---------------------------------------------------------------------------
# ingested_objects
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIngestedObjectsTable:
    """JDBC sink writes expected rows to ingested_objects after a success task."""

    def test_row_written(
        self,
        postgres_engine: sa.Engine,
        namespace_row: uuid.UUID,
        connectors: None,
        kafka_connect_url: str,
    ) -> None:
        task_id = uuid.uuid4()
        obj_id = uuid.uuid4()

        _insert_ingestion_status(
            postgres_engine,
            task_id=task_id,
            obj_id=obj_id,
            namespace_id=namespace_row,
        )

        q = "SELECT id FROM ingested_objects WHERE id = :id"
        p = {"id": obj_id}
        _assert_row(_poll_row(postgres_engine, q, p), q, p, kafka_connect_url)

    def test_all_fields_correct(
        self,
        postgres_engine: sa.Engine,
        namespace_row: uuid.UUID,
        connectors: None,
        kafka_connect_url: str,
    ) -> None:
        task_id = uuid.uuid4()
        obj_id = uuid.uuid4()
        namespace_id = namespace_row

        _insert_ingestion_status(
            postgres_engine,
            task_id=task_id,
            obj_id=obj_id,
            namespace_id=namespace_id,
        )

        q = "SELECT * FROM ingested_objects WHERE id = :id"
        p = {"id": obj_id}
        row = _assert_row(_poll_row(postgres_engine, q, p), q, p, kafka_connect_url)
        assert row.id == obj_id
        assert row.namespace_id == namespace_id
        assert row.source == "doc.txt"
        assert row.object_type == "file"
        assert row.content_type == "text/plain"
        assert row.size_bytes == 2048
        assert row.group_id is None

    def test_group_id_propagated(
        self,
        postgres_engine: sa.Engine,
        namespace_row: uuid.UUID,
        connectors: None,
        kafka_connect_url: str,
    ) -> None:
        task_id = uuid.uuid4()
        obj_id = uuid.uuid4()
        group_id = uuid.uuid4()

        _insert_ingestion_status(
            postgres_engine,
            task_id=task_id,
            obj_id=obj_id,
            namespace_id=namespace_row,
            group_id=group_id,
        )

        q = "SELECT group_id FROM ingested_objects WHERE id = :id"
        p = {"id": obj_id}
        row = _assert_row(_poll_row(postgres_engine, q, p), q, p, kafka_connect_url)
        assert row.group_id == group_id

    def test_non_index_task_not_written(
        self,
        postgres_engine: sa.Engine,
        namespace_row: uuid.UUID,
        connectors: None,
        kafka_connect_url: str,
    ) -> None:
        """tasks.ingest rows must be filtered by ksqlDB and never reach the sink."""
        filtered_task = uuid.uuid4()
        filtered_obj = uuid.uuid4()

        _insert_ingestion_status(
            postgres_engine,
            task_id=filtered_task,
            obj_id=filtered_obj,
            namespace_id=namespace_row,
            stage="tasks.ingest",
        )

        # Sentinel row confirms the pipeline is live; filtered row must not appear first.
        sentinel_task = uuid.uuid4()
        sentinel_obj = uuid.uuid4()
        _insert_ingestion_status(
            postgres_engine,
            task_id=sentinel_task,
            obj_id=sentinel_obj,
            namespace_id=namespace_row,
        )

        q = (
            "SELECT id FROM ingested_objects WHERE id IN (:filtered, :sentinel)"
            " ORDER BY ctid LIMIT 1"
        )
        p = {"filtered": filtered_obj, "sentinel": sentinel_obj}
        row = _assert_row(_poll_row(postgres_engine, q, p), q, p, kafka_connect_url)
        assert row.id == sentinel_obj, (
            f"tasks.ingest row should have been filtered out; "
            f"got {row.id!r}, expected sentinel {sentinel_obj!r}"
        )


# ---------------------------------------------------------------------------
# Delete document path
# ---------------------------------------------------------------------------
#
# Epic 22 retired ingestion_tasks (and this suite's former TestIngestionTasksTable
# / TestDeleteDocumentPath.test_delete_task_recorded+fields_correct, which asserted
# on rows landing there) — task-state visibility is now a direct read of
# ingestion_status by the storage service, not a second CDC-fed table. This class
# now only covers the tombstone path, which is unrelated to that table.


@pytest.mark.integration
class TestDeleteDocumentPath:
    """tasks.delete_document success events tombstone the ingested_objects row."""

    def test_ingested_objects_row_deleted(
        self,
        postgres_engine: sa.Engine,
        namespace_row: uuid.UUID,
        connectors: None,
        kafka_connect_url: str,
    ) -> None:
        """
        After tasks.delete_document, the ingested_objects row is removed via tombstone.
        """
        # Step 1: create the row via tasks.index
        index_task_id = uuid.uuid4()
        obj_id = uuid.uuid4()

        _insert_ingestion_status(
            postgres_engine,
            task_id=index_task_id,
            obj_id=obj_id,
            namespace_id=namespace_row,
        )
        q = "SELECT id FROM ingested_objects WHERE id = :id"
        p = {"id": obj_id}
        _assert_row(_poll_row(postgres_engine, q, p), q, p, kafka_connect_url)

        # Step 2: delete the row via tasks.delete_document
        delete_task_id = uuid.uuid4()
        _insert_ingestion_status(
            postgres_engine,
            task_id=delete_task_id,
            obj_id=obj_id,
            namespace_id=namespace_row,
            stage="tasks.delete_document",
            operation="DELETE",
        )

        deadline = time.monotonic() + _POLL_TIMEOUT_S
        while time.monotonic() < deadline:
            with postgres_engine.connect() as conn:
                row = conn.execute(sa.text(q), p).fetchone()
                if row is None:
                    return
            time.sleep(_POLL_INTERVAL_S)
        pytest.fail(
            f"ingested_objects row {obj_id!r} "
            f"was not deleted within {_POLL_TIMEOUT_S}s.\n"
            f"Connector statuses:\n{_connector_diagnostics(kafka_connect_url)}"
        )

    def test_delete_does_not_create_ingested_objects_row(
        self,
        postgres_engine: sa.Engine,
        namespace_row: uuid.UUID,
        connectors: None,
        kafka_connect_url: str,
    ) -> None:
        """tasks.delete_document rows must NOT produce ingested_objects records.

        The ksqlDB stream filter (name = 'tasks.index') prevents delete events
        from reaching the ingested_objects JDBC sink.  Confirmed by verifying
        no row appears after a sentinel index task does appear.
        """
        delete_task_id = uuid.uuid4()
        delete_obj_id = uuid.uuid4()

        _insert_ingestion_status(
            postgres_engine,
            task_id=delete_task_id,
            obj_id=delete_obj_id,
            namespace_id=namespace_row,
            stage="tasks.delete_document",
            operation="DELETE",
        )

        # Sentinel tasks.index row confirms the ingested_objects pipeline is live.
        sentinel_task = uuid.uuid4()
        sentinel_obj = uuid.uuid4()
        _insert_ingestion_status(
            postgres_engine,
            task_id=sentinel_task,
            obj_id=sentinel_obj,
            namespace_id=namespace_row,
        )
        sentinel_q = "SELECT id FROM ingested_objects WHERE id = :id"
        sentinel_p = {"id": sentinel_obj}
        _assert_row(
            _poll_row(postgres_engine, sentinel_q, sentinel_p),
            sentinel_q,
            sentinel_p,
            kafka_connect_url,
        )

        # The delete_obj_id must not have been written to ingested_objects.
        with postgres_engine.connect() as conn:
            row = conn.execute(
                sa.text("SELECT id FROM ingested_objects WHERE id = :id"),
                {"id": delete_obj_id},
            ).fetchone()
        assert row is None, (
            f"tasks.delete_document should not produce an ingested_objects row; "
            f"found id={row.id!r}"
        )
