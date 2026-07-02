"""Contract tests: Debezium CDC → artemis.celery.ingested_objects / ingestion_tasks.

Validates the ksqlDB stream logic (streams 4–6 in artemis_init.ksql) that
transforms Celery result rows (after ExtractNewRecordState unwrap) into the
two table-sink topics consumed by the Debezium JDBC sink connectors.

Contract under test
-------------------
Input  topic: apollo.ingestion.celery.results.public.apollo_celery_taskmeta
               (flat JSON row produced by debezium-cdc-postgres-task-results
                after ExtractNewRecordState SMT)

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

Output topic B: artemis.celery.ingestion_tasks
  key:   {"task_id": str (UUID)}   # PK — record_key mode; NOT duplicated in value
  value: {
    "obj_id":       str (UUID),
    "namespace_id": str (UUID),
    "status":       str,          # "SUCCESS"
    "completed_at": int,          # epoch ms (FROM_UNIXTIME of Debezium microseconds)
    "operation":    str,          # "CREATE" | "MODIFY" | "DELETE"
  }

Both topics use JSON Schema (JSON_SR) with Confluent wire format (5-byte header).
date_done is BIGINT (microseconds since epoch, Debezium TIMESTAMP encoding).

Filtered to: name = 'tasks.index' AND status = 'SUCCESS' only.

Filtered to: name = 'tasks.index' AND status = 'SUCCESS' only.
"""

from __future__ import annotations

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

_SOURCE_TOPIC = "apollo.ingestion.celery.results.public.apollo_celery_taskmeta"
_OBJECTS_TOPIC = "artemis.celery.ingested_objects"
_TASKS_TOPIC = "artemis.celery.ingestion_tasks"

# 2026-01-01T12:00:00 UTC in microseconds (Debezium TIMESTAMP encoding)
_DATE_DONE_US = 1767268800000000


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


def _make_ingestion_result(
    obj_id: str,
    namespace_id: str,
    group_id: str | None = None,
    operation: str = "CREATE",
    task_id: str | None = None,
    failure_reason: str | None = None,
) -> str:
    payload = {
        "object": {
            "id": obj_id,
            "source": "doc.txt",
            "scope": {"namespace_id": namespace_id, "group_id": group_id},
            "properties": {
                "object_type": "file",
                "content_type": "text/plain",
                "size_bytes": 2048 if operation != "DELETE" else None,
            },
        },
        "indexing": {"num_added": 0, "num_skipped": 0},
        "operation": operation,
        # Contract task_id <A> — ksqlDB keys ingestion_tasks off
        # EXTRACTJSONFIELD(result, '$.task_id'), not the taskmeta task_id
        # column (which holds the index subtask id <C>).
        "task_id": task_id,
    }
    if failure_reason is not None:
        # On FAILURE the worker's on_failure hook overwrites result with a
        # FailureRecord carrying failure_reason (mirrors the IngestionResult paths).
        payload["failure_reason"] = failure_reason
    return json.dumps(payload)


def _produce_cdc_row(
    bootstrap_server: str,
    task_id: str,
    obj_id: str,
    namespace_id: str,
    *,
    column_task_id: str | None = None,
    status: str = "SUCCESS",
    name: str = "tasks.index",
    date_done: int = _DATE_DONE_US,
    group_id: str | None = None,
    operation: str = "CREATE",
    failure_reason: str | None = None,
    result_override: str | None = None,
) -> None:
    """Produce a flat CDC row matching the ExtractNewRecordState output shape.

    ``task_id`` is the *contract* id ``<A>`` (what storage returned) and is
    carried in ``result.$.task_id`` — this is what the ksql topology keys
    ``ingestion_tasks`` off. ``column_task_id`` is the taskmeta ``task_id``
    column, which in production holds the *index subtask* id ``<C>``; it
    defaults to ``task_id`` for callers that don't care to distinguish them.

    ``result_override`` injects a raw ``result`` JSON string verbatim (used to
    simulate the pre-overwrite exception encoding Celery writes on FAILURE,
    which has no ``$.task_id`` and must be filtered out by the FAILURE fan-out).
    """
    result = (
        result_override
        if result_override is not None
        else _make_ingestion_result(
            obj_id, namespace_id, group_id, operation, task_id, failure_reason
        )
    )
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    producer.send(
        _SOURCE_TOPIC,
        value={
            "task_id": column_task_id if column_task_id is not None else task_id,
            "status": status,
            "result": result,
            "date_done": date_done,
            "traceback": None,
            "name": name,
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

    def test_object_fields_extracted_from_result_json(
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

    def test_non_index_task_not_emitted(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """Rows where name != 'tasks.index' must be filtered out."""
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(
            bootstrap_server, task_id, obj_id, namespace_id, name="tasks.ingest"
        )

        # Produce a sentinel SUCCESS tasks.index row to confirm the stream
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
        """Rows where status != 'SUCCESS' must be filtered out."""
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(
            bootstrap_server, task_id, obj_id, namespace_id, status="FAILURE"
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
        task_id = str(uuid.uuid4())  # contract id <A>, carried in result
        column_task_id = str(uuid.uuid4())  # index subtask id <C>, taskmeta column
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        # The record must key off the contract id in the result, NOT the column.
        assert record["task_id"] == task_id
        assert record["task_id"] != column_task_id

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
            # NULL on SUCCESS — projected so every producer into the stream/topic
            # shares one value schema; the FAILURE fan-out populates it.
            "failure_reason",
        }

    def test_fields_extracted_correctly(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())  # contract id <A>, carried in result
        column_task_id = str(uuid.uuid4())  # index subtask id <C>, taskmeta column
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["task_id"] != column_task_id
        assert record["obj_id"] == obj_id
        assert record["namespace_id"] == namespace_id
        assert record["status"] == "SUCCESS"
        assert record["completed_at"] is not None
        assert record["operation"] == "CREATE"


# ---------------------------------------------------------------------------
# Contract: delete_document events recorded in ingestion_tasks
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDeleteDocumentContract:
    """
    tasks.delete_document SUCCESS events are recorded in artemis.celery.ingestion_tasks.
    """

    def test_delete_record_produced_on_success(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())  # contract id <A>, carried in result
        column_task_id = str(uuid.uuid4())  # delete subtask id <C>, taskmeta column
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
            name="tasks.delete_document",
            operation="DELETE",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["task_id"] != column_task_id

    def test_delete_record_fields_correct(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())  # contract id <A>, carried in result
        column_task_id = str(uuid.uuid4())  # delete subtask id <C>, taskmeta column
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
            name="tasks.delete_document",
            operation="DELETE",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["task_id"] != column_task_id
        assert record["obj_id"] == obj_id
        assert record["namespace_id"] == namespace_id
        assert record["status"] == "SUCCESS"
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
            name="tasks.delete_document",
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
# Contract: task FAILURES recorded in ingestion_tasks (Fan-out D, §7b)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRecordFailureContract:
    """Terminal task FAILURES are recorded in artemis.celery.ingestion_tasks.

    The worker's ``FailureRecordingTask.on_failure`` overwrites the failure row's
    ``result`` with a FailureRecord JSON (so it carries the contract ``task_id`` +
    namespace_id + operation + failure_reason); the FAILURE fan-out emits a row
    keyed by the contract ``<A>`` with ``status='FAILURE'``.
    """

    @pytest.mark.parametrize(
        "name",
        ["tasks.fetch_and_parse", "tasks.index", "tasks.delete_document"],
    )
    def test_failure_row_keyed_by_contract_id(
        self, bootstrap_server: str, streams: None, name: str
    ) -> None:
        task_id = str(uuid.uuid4())  # contract id <A>, carried in result
        column_task_id = str(uuid.uuid4())  # failing subtask id <C>, taskmeta column
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
            status="FAILURE",
            name=name,
            operation="CREATE",
            failure_reason="RuntimeError: boom",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        # Keyed by the contract id in the result, NOT the taskmeta column.
        assert record["task_id"] == task_id
        assert record["task_id"] != column_task_id
        assert record["status"] == "FAILURE"
        assert record["namespace_id"] == namespace_id
        assert record["obj_id"] == obj_id
        assert record["operation"] == "CREATE"
        assert record["failure_reason"] == "RuntimeError: boom"
        assert record["completed_at"] is not None

    def test_double_write_guard_drops_exception_encoded_row(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """The pre-overwrite event (Celery's exception encoding, no ``$.task_id``)
        must NOT produce an ``ingestion_tasks`` record.

        Produce the exception-encoded row FIRST then the on_failure overwrite for
        the SAME failing subtask; the first (and only) emitted record must be the
        overwrite, keyed by the contract id — proving the ``task_id IS NOT NULL``
        guard filtered the exception-encoded event.
        """
        task_id = str(uuid.uuid4())  # contract id <A>
        column_task_id = str(uuid.uuid4())  # the failing subtask's row id <C>
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        # 1. mark_as_failure: exception encoding, NO $.task_id → must be filtered.
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
            status="FAILURE",
            name="tasks.index",
            result_override=json.dumps(
                {
                    "exc_type": "RuntimeError",
                    "exc_message": ["boom"],
                    "exc_module": "builtins",
                }
            ),
        )
        # 2. on_failure overwrite: FailureRecord carrying the contract id.
        _produce_cdc_row(
            bootstrap_server,
            task_id,
            obj_id,
            namespace_id,
            column_task_id=column_task_id,
            status="FAILURE",
            name="tasks.index",
            operation="CREATE",
            failure_reason="RuntimeError: boom",
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        # If the guard were missing, the FIRST emitted record would be the
        # exception-encoded one with a null task_id key.
        assert record["task_id"] == task_id
        assert record["status"] == "FAILURE"
        assert record["failure_reason"] == "RuntimeError: boom"
