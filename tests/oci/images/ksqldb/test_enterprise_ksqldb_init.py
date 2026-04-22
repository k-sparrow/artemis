"""Integration tests: artemis/ksqldb-enterprise-init sidecar creates ksqlDB streams.

Verifies that the ``artemis/ksqldb-enterprise-init:latest`` one-shot container:
  1. Exits cleanly (exit code 0).
  2. Creates the ``artemis-enterprise-datasource-filesystem`` source stream.
  3. Creates the ``artemis-enterprise-datasource-filesystem-intake`` derived stream.
  4. Correctly promotes Kafka record headers into the IntakeRequest JSON value
     (FROM_BYTES decoding, STRUCT construction for the source field).

Input topic:  artemis.datasource.filesystem        (KAFKA bytes + headers)
Output topic: artemis.datasource.filesystem.intake  (JSON, IntakeRequest schema)

Header contract (set by FileSource connector + artemis SMT):
    CamelHeader.CamelFileAbsolutePath  → source.path
    CamelHeader.CamelFileName          → display_name
    artemis.namespace_id               → namespace_id

Prerequisites:
    bazel run //tools/oci/images/ksqldb:enterprise_init_tarball
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

_INPUT_TOPIC = "artemis.datasource.filesystem"
_OUTPUT_TOPIC = "artemis.datasource.filesystem.intake"

_INIT_IMAGE = "artemis/ksqldb-enterprise-init:latest"
_DONE_LOG = r"✓ Enterprise ksqlDB streams created\."


# ---------------------------------------------------------------------------
# Topic pre-creation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def enterprise_topics(bootstrap_server: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    existing = set(admin.list_topics())
    new = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in (_INPUT_TOPIC, _OUTPUT_TOPIC)
        if t not in existing
    ]
    if new:
        try:
            admin.create_topics(new)
        except TopicAlreadyExistsError:
            pass
    admin.close()


# ---------------------------------------------------------------------------
# Init sidecar
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def enterprise_init_exit_code(
    network: Network,
    ksqldb: KSQLDbContainer,
    ksqldb_internal_url: str,
    enterprise_topics: None,
) -> int:
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


def _produce_file_event(
    bootstrap_server: str,
    abs_path: str,
    filename: str,
    namespace: str,
    namespace_id: str,
    org_name: str,
) -> None:
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    producer.send(
        _INPUT_TOPIC,
        value=b"",  # VALUE_FORMAT=KAFKA — content ignored; headers carry the data
        headers=[
            ("CamelHeader.CamelFileAbsolutePath", abs_path.encode()),
            ("CamelHeader.CamelFileName", filename.encode()),
            ("artemis.namespace", namespace.encode()),
            ("artemis.namespace_id", namespace_id.encode()),
            ("artemis.org_name", org_name.encode()),
        ],
    )
    producer.flush()
    producer.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestEnterpriseKsqlDbInit:
    def test_init_exits_cleanly(self, enterprise_init_exit_code: int) -> None:
        assert enterprise_init_exit_code == 0

    def test_filesystem_source_stream_exists(
        self, ksqldb_url: str, enterprise_init_exit_code: int
    ) -> None:
        streams = _show_streams(ksqldb_url)
        assert "artemis-enterprise-datasource-filesystem" in streams

    def test_filesystem_intake_stream_exists(
        self, ksqldb_url: str, enterprise_init_exit_code: int
    ) -> None:
        streams = _show_streams(ksqldb_url)
        assert "artemis-enterprise-datasource-filesystem-intake" in streams

    def test_file_path_promoted_to_source(
        self, bootstrap_server: str, enterprise_init_exit_code: int
    ) -> None:
        abs_path = f"/watch/{uuid.uuid4().hex}.txt"
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server, abs_path, "doc.txt", "test-ns", namespace_id, "test-org"
        )

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["source"]["path"] == abs_path
        assert record["source"]["type"] == "filesystem"

    def test_filename_promoted_to_display_name(
        self, bootstrap_server: str, enterprise_init_exit_code: int
    ) -> None:
        filename = f"report-{uuid.uuid4().hex[:8]}.pdf"
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            f"/watch/{filename}",
            filename,
            "test-ns",
            namespace_id,
            "test-org",
        )

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["display_name"] == filename

    def test_namespace_id_promoted_to_value(
        self, bootstrap_server: str, enterprise_init_exit_code: int
    ) -> None:
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            f"/watch/{uuid.uuid4().hex}.txt",
            "doc.txt",
            "test-ns",
            namespace_id,
            "test-org",
        )

        record = _consume_from(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["namespace_id"] == namespace_id

    def test_multiple_files_each_transformed_independently(
        self, bootstrap_server: str, enterprise_init_exit_code: int
    ) -> None:
        files = [
            (f"/watch/{uuid.uuid4().hex}.txt", f"file-{i}.txt", str(uuid.uuid4()))
            for i in range(3)
        ]

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        for abs_path, filename, namespace_id in files:
            _produce_file_event(
                bootstrap_server,
                abs_path,
                filename,
                "test-ns",
                namespace_id,
                "test-org",
            )

        tp = TopicPartition(_OUTPUT_TOPIC, 0)
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_server],
            enable_auto_commit=False,
            consumer_timeout_ms=30_000,
            value_deserializer=lambda b: json.loads(b.decode()),
            group_id=None,
        )
        consumer.assign([tp])
        consumer.poll(timeout_ms=500)
        consumer.seek(tp, start)

        seen_namespace_ids = set()
        try:
            for record in consumer:
                seen_namespace_ids.add(record.value["namespace_id"])
                if len(seen_namespace_ids) == len(files):
                    break
        finally:
            consumer.close()

        expected = {ns for _, _, ns in files}
        assert seen_namespace_ids == expected
