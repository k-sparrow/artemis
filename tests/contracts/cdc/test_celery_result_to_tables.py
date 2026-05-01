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
  {
    "id":           str (UUID),   # obj_id — PK for ingested_objects table
    "namespace_id": str (UUID),
    "source":       str,
    "object_type":  str,
    "s3_key":       str,          # "{namespace_id}/{obj_id}"
    "content_type": str,
    "size_bytes":   int | null,
    "group_id":     str | null,   # UUID
  }

Output topic B: artemis.celery.ingestion_tasks
  {
    "task_id":      str (UUID),   # PK for ingestion_tasks table
    "obj_id":       str (UUID),
    "namespace_id": str (UUID),
    "status":       str,          # "SUCCESS"
    "completed_at": str,          # date_done pass-through
  }

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


def _consume_one(
    bootstrap_server: str, topic: str, start_offset: int, timeout_s: int = 30
) -> dict:
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_s * 1_000,
        value_deserializer=lambda b: json.loads(b.decode()),
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=500)
    consumer.seek(tp, start_offset)
    try:
        for record in consumer:
            return record.value
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


def _make_ingestion_result(
    obj_id: str, namespace_id: str, group_id: str | None = None
) -> str:
    return json.dumps(
        {
            "object": {
                "id": obj_id,
                "source": "doc.txt",
                "scope": {"namespace_id": namespace_id, "group_id": group_id},
                "properties": {
                    "object_type": "file",
                    "content_type": "text/plain",
                    "size_bytes": 2048,
                },
            },
            "indexing": {"num_added": 3, "num_skipped": 0, "ids": ["a", "b", "c"]},
        }
    )


def _produce_cdc_row(
    bootstrap_server: str,
    task_id: str,
    obj_id: str,
    namespace_id: str,
    *,
    status: str = "SUCCESS",
    name: str = "tasks.index",
    date_done: str = "2026-01-01T12:00:00",
    group_id: str | None = None,
) -> None:
    """Produce a flat CDC row matching the ExtractNewRecordState output shape."""
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    producer.send(
        _SOURCE_TOPIC,
        value={
            "task_id": task_id,
            "status": status,
            "result": _make_ingestion_result(obj_id, namespace_id, group_id),
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
            "s3_key",
            "content_type",
            "size_bytes",
            "group_id",
        }

    def test_s3_key_derived_correctly(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OBJECTS_TOPIC)
        _produce_cdc_row(bootstrap_server, task_id, obj_id, namespace_id)

        record = _consume_one(bootstrap_server, _OBJECTS_TOPIC, start)
        assert record["s3_key"] == f"{namespace_id}/{obj_id}"

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
        }

    def test_fields_extracted_correctly(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())
        date_done = "2026-04-30T09:15:00"

        start = _end_offset(bootstrap_server, _TASKS_TOPIC)
        _produce_cdc_row(
            bootstrap_server, task_id, obj_id, namespace_id, date_done=date_done
        )

        record = _consume_one(bootstrap_server, _TASKS_TOPIC, start)
        assert record["task_id"] == task_id
        assert record["obj_id"] == obj_id
        assert record["namespace_id"] == namespace_id
        assert record["status"] == "SUCCESS"
        assert record["completed_at"] == date_done
