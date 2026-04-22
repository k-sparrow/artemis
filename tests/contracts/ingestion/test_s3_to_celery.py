"""Contract tests: artemis.ingestion.storage.s3 → artemis.ingestion.celery.tasks.

Validates the ksqlDB stream logic that transforms MinIO S3 events into the
Celery v2 task message format consumed by the `tasks.ingest` worker.

The streams are created directly via the ksqlDB REST API — these tests are
decoupled from the `artemis/ksqldb-init` OCI image and validate *only* the
stream contract defined in tools/oci/images/ksqldb/artemis_init.ksql.

Contract under test
-------------------
Input  topic: artemis.ingestion.storage.s3  (JSON, MinIO event envelope)
Output topic: artemis.ingestion.celery.tasks (JSON, Celery v2 kwargs)

Output message structure
------------------------
  {
    "task_id": "<uuid>",        # top-level — HeaderFrom SMT promotes to CamelHeader.id
    "args":    [],              # Celery v2: always empty
    "kwargs": {
      "s3":            {"bucket": str, "object": str},
      "source":        {
                          "source": str,
                          "content_type":
                          str, "obj_id": str
                          "object_type": str
                        },
      "upload_action": "CREATE" | "MODIFY" | "DELETE",  # uppercased
      "info":          {"namespace_id": str},
    }
  }

Partition key: namespace_id (JSON-encoded string, KEY_FORMAT=JSON)

Infrastructure: KafkaContainer + KSQLDbContainer only (see conftest.py).
"""

from __future__ import annotations

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

_INPUT_TOPIC = "artemis.ingestion.storage.s3"
_OUTPUT_TOPIC = "artemis.ingestion.celery.tasks"

_REQUIRED_TOP_LEVEL_KEYS = {"task_id", "args", "kwargs"}
_REQUIRED_KWARGS_KEYS = {"s3", "source", "upload_action", "info"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_s3_event(
    task_id: str,
    namespace_id: str,
    bucket: str,
    obj_key: str,
    upload_action: str = "create",
) -> dict:
    contract = json.dumps(
        {
            "upload_action": upload_action,
            "s3": {"bucket": bucket, "object": obj_key},
            "source": {
                "source": "storage",
                "content_type": "text/plain",
                "obj_id": str(uuid.uuid4()),
                "object_type": "file",
            },
            "info": {"namespace_id": namespace_id},
        }
    )
    return {
        "EventName": f"s3:ObjectCreated:{upload_action.capitalize()}",
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {
                        "key": obj_key,
                        "userMetadata": {
                            "X-Amz-Meta-Task_id": task_id,
                            "X-Amz-Meta-Contract": contract,
                        },
                    },
                }
            }
        ],
    }


def _end_offset(bootstrap_server: str, topic: str) -> int:
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=500)
    offsets = consumer.end_offsets([tp])
    consumer.close()
    return offsets[tp]


def _consume_one(
    bootstrap_server: str, topic: str, start_offset: int, timeout_s: int = 30
) -> dict:
    """Return the first record value at or after *start_offset*."""
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_s * 1000,
        value_deserializer=lambda b: json.loads(b.decode()),
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=500)
    consumer.seek(tp, start_offset)
    try:
        for record in consumer:
            return record
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


def _produce_s3_event(
    bootstrap_server: str,
    task_id: str,
    namespace_id: str,
    upload_action: str = "create",
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    producer.send(
        _INPUT_TOPIC,
        value=_make_s3_event(
            task_id,
            namespace_id,
            "artemis",
            f"{namespace_id}/doc.txt",
            upload_action,
        ),
    )
    producer.flush()
    producer.close()


# ---------------------------------------------------------------------------
# Contract: S3 input parsing
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestS3InputContract:
    """The source stream correctly parses MinIO S3 events for all upload actions."""

    @pytest.mark.parametrize("upload_action", ["create", "delete"])
    def test_event_produces_output(
        self, bootstrap_server: str, streams: None, upload_action: str
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id, upload_action)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record.value["task_id"] == task_id


# ---------------------------------------------------------------------------
# Contract: Celery v2 output envelope
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCeleryOutputContract:
    """Output records conform to the Celery v2 task message envelope."""

    def test_top_level_keys_are_complete(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert set(record.value.keys()) == _REQUIRED_TOP_LEVEL_KEYS

    def test_args_is_empty_list(self, bootstrap_server: str, streams: None) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record.value["args"] == []

    def test_kwargs_keys_are_complete(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert set(record.value["kwargs"].keys()) == _REQUIRED_KWARGS_KEYS

    def test_task_id_is_top_level_for_headerfrom_smt(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """
        task_id must be a top-level field; the HeaderFrom SMT reads it from the value.
        """
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record.value["task_id"] == task_id
        assert "task_id" not in record.value.get("kwargs", {})

    @pytest.mark.parametrize("upload_action", ["create", "delete"])
    def test_upload_action_is_uppercased(
        self, bootstrap_server: str, streams: None, upload_action: str
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id, upload_action)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record.value["kwargs"]["upload_action"] == upload_action.upper()

    def test_partition_key_is_namespace_id(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """
        Kafka record key must be JSON-encoded
        namespace_id (KEY_FORMAT=JSON + PARTITION BY).
        ksqlDB strips the partition field from the value,
        so namespace_id only appears in the key.
        """
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        # KEY_FORMAT=JSON encodes the string as a JSON value (quoted)
        key = json.loads(record.key.decode())
        assert key == namespace_id

    def test_s3_bucket_and_object_extracted(
        self, bootstrap_server: str, streams: None
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())
        obj_key = f"{namespace_id}/report.pdf"

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(task_id, namespace_id, "artemis", obj_key),
        )
        producer.flush()
        producer.close()

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        s3 = record.value["kwargs"]["s3"]
        assert s3["bucket"] == "artemis"
        assert s3["object"] == obj_key

    def test_namespace_id_in_info(self, bootstrap_server: str, streams: None) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_s3_event(bootstrap_server, task_id, namespace_id)

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record.value["kwargs"]["info"]["namespace_id"] == namespace_id
