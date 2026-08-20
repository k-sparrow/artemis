"""Contract tests: Debezium CDC → artemis.celery.ingested_objects / ingestion_tasks.

Validates the ksqlDB stream logic (streams 4–8 in artemis_init.ksql) that
transforms ingestion_status CDC rows (after ExtractNewRecordState unwrap)
into the two table-sink topics consumed by the Debezium JDBC sink connectors.

Contract under test
-------------------
Input  topic: apollo.ingestion.celery.results.public.ingestion_status
               (flat JSON row produced by debezium-cdc-postgres-task-results
                after ExtractNewRecordState SMT — matches the ingestion_status
                table columns 1:1, see
                src/backend/controller/worker/backend/outbox.py)

Output topic A: artemis.celery.ingested_objects
  key:   {"id": str (UUID)}        # PK — record_key mode; NOT duplicated in value
  value: {
    "namespace_id": str (UUID),
    "source":       str,
    "object_type":  str,
    "content_type": str,
    "size_bytes":   int | null,
    "group_id":     str | null,   # UUID
  }
  Filtered to: stage = 'tasks.index' AND status = 'success'.

Output topic B: artemis.celery.ingestion_tasks
  key:   {"task_id": str (UUID)}   # PK — record_key mode; NOT duplicated in value
  value: {
    "obj_id":       str (UUID),
    "namespace_id": str (UUID),
    "status":       str,          # "success" | "failure"
    "completed_at": int,          # epoch ms (FROM_UNIXTIME of Debezium microseconds)
    "operation":    str,          # "CREATE" | "MODIFY" | "DELETE"
    "failure_reason": str | null,
  }
  Filtered to: (stage IN ('tasks.index', 'tasks.delete_document') AND status = 'success')
               OR status = 'failure' — no task-name whitelist on the failure path;
               ingestion_status is keyed by the contract task_id from row creation,
               so every row IS the contract id, with no subtask-id disambiguation
               needed (unlike the old apollo_celery_taskmeta-based topology).

Both topics use JSON Schema (JSON_SR) with Confluent wire format (5-byte header).
updated_at is BIGINT (microseconds since epoch, Debezium TIMESTAMP encoding).
"""

from __future__ import annotations

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

_SOURCE_TOPIC = "apollo.ingestion.celery.results.public.ingestion_status"
_OBJECTS_TOPIC = "artemis.celery.ingested_objects"
_TASKS_TOPIC = "artemis.celery.ingestion_tasks"

# 2026-01-01T12:00:00 UTC in microseconds (Debezium TIMESTAMP encoding)
_UPDATED_AT_US = 1767268800000000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _end_offset(bootstrap_server: str, topic: str) -> int:
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=3_000,
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=500)
    offset = consumer.end_offsets([tp])[tp]
    consumer.close()
    return offset


def _decode_sr(b: bytes | None) -> dict:
    """
    Strip the 5-byte Confluent wire format header
    (magic byte + schema ID) and JSON-parse.
    """
    if not b:
        return {}
    return json.loads(b[5:].decode())


def _consume_one(
    bootstrap_server: str, topic: str, start_offset: int, timeout_s: int = 30
) -> dict:
    """Consume one record from *topic* at or after *start_offset*.

    Returns key fields merged into value fields so callers can assert on PK
    fields (id, task_id) alongside value fields without knowing which side holds them.
    Both key and value use JSON_SR (Confluent wire format: 5-byte header + JSON payload).
    """
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_s * 1_000,
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=500)
    consumer.seek(tp, start_offset)
    try:
        for record in consumer:
            return {**_decode_sr(record.key), **_decode_sr(record.value)}
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


def _consume_raw(
    bootstrap_server: str, topic: str, start_offset: int, timeout_s: int = 30
) -> tuple[bytes | None, bytes | None]:
    """
    Return (raw_key_bytes, raw_value_bytes) for the first record
    at or after start_offset.

    Used for tombstone detection: a tombstone has value=None (null bytes).
    """
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_s * 1_000,
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=500)
    consumer.seek(tp, start_offset)
    try:
        for record in consumer:
            return record.key, record.value
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


def _produce_cdc_row(
    bootstrap_server: str,
    task_id: str,
    obj_id: str,
    namespace_id: str,
    *,
    stage: str = "tasks.index",
    status: str = "success",
    updated_at: int = _UPDATED_AT_US,
    group_id: str | None = None,
    operation: str = "CREATE",
    failure_reason: str | None = None,
    source: str = "doc.txt",
    object_type: str = "file",
    content_type: str = "text/plain",
    size_bytes: int | None = 2048,
) -> None:
    """Produce a flat CDC row matching the ExtractNewRecordState output shape
    for ingestion_status — one row per contract task_id, updated in place.
    """
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    producer.send(
        _SOURCE_TOPIC,
        value={
            "task_id": task_id,
            "namespace_id": namespace_id,
            "obj_id": obj_id,
            "source": source,
            "object_type": object_type,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "group_id": group_id,
            "operation": operation,
            "stage": stage,
            "status": status,
            "failure_reason": failure_reason,
            "updated_at": updated_at,
        },
    )
    producer.flush()
    producer.close()


# ---------------------------------------------------------------------------
# Contract: ingested_objects output shape
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIngestedObjectsContract:
    """artemis.celery.ingested_objects receives correctly shaped records."""

    def test_record_produced_on_success(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert record["id"] == obj_id

    def test_required_fields_present(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert set(record.keys()) == {
            "id",
            "namespace_id",
            "source",
            "object_type",
            "content_type",
            "size_bytes",
            "group_id",
        }

    def test_object_fields_projected(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert record["namespace_id"] == namespace_id
        assert record["source"] == "doc.txt"
        assert record["object_type"] == "file"
        assert record["content_type"] == "text/plain"
        assert record["size_bytes"] == 2048
        assert record["group_id"] is None

    def test_group_id_propagated_when_set(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())
        group_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(
            bootstrap_server, task_id, obj_id, namespace_id, group_id=group_id
        )

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert record["group_id"] == group_id

    def test_non_index_stage_not_emitted(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """Rows where stage != 'tasks.index' must be filtered out."""
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(
            bootstrap_server, task_id, obj_id, namespace_id, stage="tasks.ingest"
        )

        # Produce a sentinel success tasks.index row to confirm the stream
        # is alive — if the ingest row leaked through it would appear first.
        sentinel_obj = str(uuid.uuid4())
        sentinel_task = str(uuid.uuid4())
        sentinel_ns = str(uuid.uuid4())
        _produce_cdc_row(bootstrap_server, sentinel_task, sentinel_obj, sentinel_ns)

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert record["id"] == sentinel_obj, (
            "tasks.ingest row should have been filtered; "
            f"got id={record['id']!r}, expected sentinel {sentinel_obj!r}"
        )

    def test_non_success_status_not_emitted(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """Rows where status != 'success' must be filtered out."""
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(
            bootstrap_server, task_id, obj_id, namespace_id, status="running"
        )

        sentinel_obj = str(uuid.uuid4())
        sentinel_task = str(uuid.uuid4())
        sentinel_ns = str(uuid.uuid4())
        _produce_cdc_row(bootstrap_server, sentinel_task, sentinel_obj, sentinel_ns)

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert record["id"] == sentinel_obj


# ---------------------------------------------------------------------------
# Contract: ingestion_tasks output shape
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIngestionTasksContract:
    """artemis.celery.ingestion_tasks receives correctly shaped records."""

    def test_record_produced_on_success(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id

    def test_required_fields_present(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert set(record.keys()) == {
            "task_id",
            "obj_id",
            "namespace_id",
            "status",
            "completed_at",
            "operation",
            # NULL on success — projected so every producer into the stream/topic
            # shares one value schema; the failure fan-out populates it.
            "failure_reason",
        }

    def test_fields_projected_correctly(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["obj_id"] == obj_id
        assert record["namespace_id"] == namespace_id
        assert record["status"] == "success"
        assert record["completed_at"] is not None
        assert record["operation"] == "CREATE"


# ---------------------------------------------------------------------------
# Contract: delete_document events recorded in ingestion_tasks
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDeleteDocumentContract:
    """
    tasks.delete_document success events are recorded in artemis.celery.ingestion_tasks.
    """

    def test_delete_record_produced_on_success(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            stage="tasks.delete_document",
            operation="DELETE",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id

    def test_delete_record_fields_correct(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            stage="tasks.delete_document",
            operation="DELETE",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["obj_id"] == obj_id
        assert record["namespace_id"] == namespace_id
        assert record["status"] == "success"
        assert record["operation"] == "DELETE"
        assert record["completed_at"] is not None

    def test_tombstone_produced_on_ingested_objects(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """
        tasks.delete_document produces a tombstone on artemis.celery.ingested_objects.
        """
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            stage="tasks.delete_document",
            operation="DELETE",
        )

        key_bytes, value_bytes = _consume_raw(bootstrap_server, _OBJECTS_TOPIC, start)
        assert value_bytes is None, (
            f"Expected tombstone (null value) on {_OBJECTS_TOPIC}; "
            f"got non-null value bytes"
        )
        key = _decode_sr(key_bytes)
        assert key["id"] == obj_id


# ---------------------------------------------------------------------------
# Contract: task failures recorded in ingestion_tasks (Fan-out C, §7)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRecordFailureContract:
    """Terminal task failures are recorded in artemis.celery.ingestion_tasks.

    No task-name whitelist: the fan-out is a plain ``WHERE status = 'failure'``
    filter (see artemis_init.ksql §7) — every row in ingestion_status is
    already keyed by the contract task_id from creation, so any ``stage`` value
    reaches ingestion_tasks on failure, including one that would never have
    been on the old whitelist. That's the point of this test — it proves the
    filter is structural, not enumerated.
    """

    def test_failure_row_recorded_for_arbitrary_stage(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            stage="tasks.some_future_task_never_whitelisted",
            status="failure",
            operation="CREATE",
            failure_reason="RuntimeError: boom",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["status"] == "failure"
        assert record["namespace_id"] == namespace_id
        assert record["obj_id"] == obj_id
        assert record["operation"] == "CREATE"
        assert record["failure_reason"] == "RuntimeError: boom"
        assert record["completed_at"] is not None
