"""Integration tests: artemis/ksqldb-init sidecar creates ksqlDB streams.

Verifies that the ``artemis/ksqldb-init:latest`` one-shot container:
  1. Exits cleanly (exit code 0).
  2. Creates the ``artemis-ingestion-storage-s3`` source stream.
  3. Creates the ``artemis-ingestion-celery-tasks`` derived stream.
  4. Correctly transforms a MinIO S3 event into the Celery v2 task format
     (field extraction, UCASE on upload_action, STRUCT construction).

Input topic:  artemis.ingestion.storage.s3  (JSON, MinIO event envelope)
Output topic: artemis.ingestion.celery.tasks  (JSON, Celery v2 kwargs)

Prerequisites:
    bazel run //tools/oci/images/ksqldb:artemis_init_tarball
"""

from __future__ import annotations

import json
import uuid

import httpx
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from tests.lib.testcontainers.kafka import KSQLDbContainer

_INPUT_TOPIC = "artemis.ingestion.storage.s3"
_OUTPUT_TOPIC = "artemis.ingestion.celery.tasks"
_ROUTED_TOPIC = "artemis.ingestion.celery.tasks.routed"

_INIT_IMAGE = "artemis/ksqldb-init:latest"
_DONE_LOG = r"✓ Ingestion ksqlDB streams created\."


# ---------------------------------------------------------------------------
# Topic pre-creation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def topics(bootstrap_server: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    existing = set(admin.list_topics())
    new = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in (_INPUT_TOPIC, _OUTPUT_TOPIC, _ROUTED_TOPIC)
        if t not in existing
    ]
    if new:
        try:
            admin.create_topics(new)
        except TopicAlreadyExistsError:
            pass
    admin.close()


# ---------------------------------------------------------------------------
# Init sidecar (session-scoped — run once, streams persist for all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def init_exit_code(
    network: Network,
    ksqldb: KSQLDbContainer,
    ksqldb_internal_url: str,
    topics: None,
) -> int:
    """Run the init container and return its exit code."""
    container = (
        DockerContainer(_INIT_IMAGE)
        .with_network(network)
        .with_env("KSQLDB_SERVER_URL", ksqldb_internal_url)
    )
    container.waiting_for(LogMessageWaitStrategy(_DONE_LOG).with_startup_timeout(120))
    container.start()
    result = container.get_wrapped_container().wait(timeout=30)
    container.stop()
    return result["StatusCode"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _show_streams(ksqldb_url: str) -> set[str]:
    resp = httpx.post(
        f"{ksqldb_url}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        json={"ksql": "SHOW STREAMS;"},
        timeout=10.0,
    )
    resp.raise_for_status()
    return {s["name"] for s in resp.json()[0]["streams"]}


def _make_s3_event(task_id: str, namespace_id: str, bucket: str, obj_key: str) -> dict:
    contract = json.dumps(
        {
            "upload_action": "create",
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
        "EventName": "s3:ObjectCreated:Put",
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


def _consume_from(
    bootstrap_server: str, topic: str, start_offset: int, timeout_s: int = 30
) -> dict:
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
            return record.value
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


def _consume_record_from(
    bootstrap_server: str, topic: str, start_offset: int, timeout_s: int = 30
) -> tuple[bytes | None, dict]:
    """Return (raw_key_bytes, deserialized_value) for the first record >= start_offset."""
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
            return record.key, record.value
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestArtemisKsqlDbInit:
    def test_init_exits_cleanly(self, init_exit_code: int) -> None:
        assert init_exit_code == 0

    def test_source_stream_exists(self, ksqldb_url: str, init_exit_code: int) -> None:
        streams = _show_streams(ksqldb_url)
        assert "artemis-ingestion-storage-s3" in streams

    def test_celery_tasks_stream_exists(
        self, ksqldb_url: str, init_exit_code: int
    ) -> None:
        streams = _show_streams(ksqldb_url)
        assert "artemis-ingestion-celery-tasks" in streams

    def test_s3_event_task_id_passed_through(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["task_id"] == task_id

    def test_s3_event_args_is_empty_list(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["args"] == []

    def test_s3_event_upload_action_uppercased(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["kwargs"]["upload_action"] == "CREATE"

    def test_s3_event_s3_fields_extracted(
        self, bootstrap_server: str, init_exit_code: int
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

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        s3 = record["kwargs"]["s3"]
        assert s3["bucket"] == "artemis"
        assert s3["object"] == obj_key

    def test_s3_event_namespace_id_in_info(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["kwargs"]["info"]["namespace_id"] == namespace_id

    def test_s3_event_source_fields_extracted(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        source = record["kwargs"]["source"]
        assert source["content_type"] == "text/plain"
        assert source["object_type"] == "file"

    # ------------------------------------------------------------------
    # Routed stream: artemis.ingestion.celery.tasks.routed
    # Verifies that task_id becomes the Kafka record KEY (JSON-encoded
    # string) and is stripped from the VALUE, leaving args + kwargs only.
    # This is the shape required by HoistField$Key + HeaderFrom$Key on
    # the RabbitMQ sink connector so Celery receives the correct task ID.
    # ------------------------------------------------------------------

    def test_celery_tasks_routed_stream_exists(
        self, ksqldb_url: str, init_exit_code: int
    ) -> None:
        streams = _show_streams(ksqldb_url)
        assert "artemis-ingestion-celery-tasks-routed" in streams

    def test_routed_task_id_is_record_key(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        """
        task_id must be the Kafka record KEY as a named struct {"task_id": "<uuid>"}.
        """
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _ROUTED_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        raw_key, _ = _consume_record_from(bootstrap_server, _ROUTED_TOPIC, start)
        # PARTITION BY STRUCT produces a named key: {"task_id": "<uuid>"}
        assert raw_key is not None
        key_value = json.loads(raw_key.decode())
        assert key_value == {"task_id": task_id}

    def test_routed_task_id_absent_from_value(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        """task_id must NOT appear in the message VALUE — it lives in the KEY only."""
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _ROUTED_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        _, value = _consume_record_from(bootstrap_server, _ROUTED_TOPIC, start)
        assert "task_id" not in value

    def test_routed_value_contains_args_and_kwargs(
        self, bootstrap_server: str, init_exit_code: int
    ) -> None:
        """VALUE must contain args (empty list) and kwargs (nested task payload)."""
        task_id = str(uuid.uuid4())
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _ROUTED_TOPIC)

        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value=_make_s3_event(
                task_id, namespace_id, "artemis", f"{namespace_id}/doc.txt"
            ),
        )
        producer.flush()
        producer.close()

        _, value = _consume_record_from(bootstrap_server, _ROUTED_TOPIC, start)
        assert value["args"] == []
        assert "kwargs" in value
        assert value["kwargs"]["upload_action"] == "CREATE"
        assert value["kwargs"]["info"]["namespace_id"] == namespace_id
