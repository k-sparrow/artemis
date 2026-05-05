"""Contract tests: artemis.datasource.filesystem → artemis.datasource.filesystem.intake.

Validates the ksqlDB enterprise CSAS (artemis_enterprise_init.ksql) that promotes
Kafka headers injected by the Camel FileSource connector + artemis SMT into an
IntakeRequest JSON body consumed by the HTTP sink → POST /intake.

Contract under test
-------------------
Input  topic: artemis.datasource.filesystem
  Headers:
    CamelHeader.CamelFileAbsolutePath : bytes — absolute path on the watched FS
    CamelHeader.CamelFileName         : bytes — filename (display name)
    artemis.namespace                 : bytes — namespace name
    artemis.namespace_id              : bytes — namespace UUID
    artemis.org_name                  : bytes — organisation name
    artemis.group_id                  : bytes | absent — connector id (UUID)

Output topic: artemis.datasource.filesystem.intake  (VALUE_FORMAT='JSON')
  value: {
    "source":       {"type": "filesystem", "path": str},
    "display_name": str,
    "namespace_id": str (UUID),
    "group_id":     str | null,
  }
"""

from __future__ import annotations

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

_SOURCE_TOPIC = "artemis.datasource.filesystem"
_OUTPUT_TOPIC = "artemis.datasource.filesystem.intake"


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
    """Consume one JSON record from *topic* at or after *start_offset*."""
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
            return json.loads(record.value.decode())
    finally:
        consumer.close()
    pytest.fail(f"No record on {topic} after offset {start_offset} within {timeout_s}s")


def _produce_file_event(
    bootstrap_server: str,
    *,
    path: str,
    display_name: str,
    namespace: str,
    namespace_id: str,
    org_name: str,
    group_id: str | None,
) -> None:
    """Produce a raw FileSource event to the source topic with the artemis SMT headers."""
    headers = [
        ("CamelHeader.CamelFileAbsolutePath", path.encode()),
        ("CamelHeader.CamelFileName", display_name.encode()),
        ("artemis.namespace", namespace.encode()),
        ("artemis.namespace_id", namespace_id.encode()),
        ("artemis.org_name", org_name.encode()),
    ]
    if group_id is not None:
        headers.append(("artemis.group_id", group_id.encode()))

    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    producer.send(
        _SOURCE_TOPIC,
        value=b"",  # FileSource payload is the raw file bytes; content irrelevant here
        headers=headers,
    )
    producer.flush()
    producer.close()


# ---------------------------------------------------------------------------
# Contract: enterprise filesystem CSAS output shape
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFilesystemStreamContract:
    """
    artemis.datasource.filesystem.intake receives correctly shaped IntakeRequest records.
    """

    def test_record_produced_on_file_event(
        self, bootstrap_server: str, streams: None
    ) -> None:
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            path="/watch/enterprise/doc.txt",
            display_name="doc.txt",
            namespace="e2e-ns",
            namespace_id=namespace_id,
            org_name="e2e-org",
            group_id=None,
        )

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record is not None

    def test_required_fields_present(
        self, bootstrap_server: str, streams: None
    ) -> None:
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            path="/watch/enterprise/doc.txt",
            display_name="doc.txt",
            namespace="e2e-ns",
            namespace_id=namespace_id,
            org_name="e2e-org",
            group_id=None,
        )

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert set(record.keys()) == {
            "source",
            "display_name",
            "namespace_id",
            "group_id",
        }

    def test_source_struct_shape(self, bootstrap_server: str, streams: None) -> None:
        namespace_id = str(uuid.uuid4())
        path = "/watch/enterprise/report.pdf"

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            path=path,
            display_name="report.pdf",
            namespace="e2e-ns",
            namespace_id=namespace_id,
            org_name="e2e-org",
            group_id=None,
        )

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["source"]["type"] == "filesystem"
        assert record["source"]["path"] == path

    def test_headers_promoted_to_fields(
        self, bootstrap_server: str, streams: None
    ) -> None:
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            path="/watch/enterprise/notes.md",
            display_name="notes.md",
            namespace="e2e-ns",
            namespace_id=namespace_id,
            org_name="e2e-org",
            group_id=None,
        )

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["display_name"] == "notes.md"
        assert record["namespace_id"] == namespace_id

    def test_group_id_null_when_header_absent(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """group_id must be null when the artemis.group_id header is not set.

        Private uploads and any connector that omits the header must not bleed
        a non-null group_id into the intake request.
        """
        namespace_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            path="/watch/enterprise/doc.txt",
            display_name="doc.txt",
            namespace="e2e-ns",
            namespace_id=namespace_id,
            org_name="e2e-org",
            group_id=None,
        )

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["group_id"] is None

    def test_group_id_propagated_from_header(
        self, bootstrap_server: str, streams: None
    ) -> None:
        """group_id from the artemis.group_id header must appear in the output JSON.

        This is the core Epic 11 contract: the connector id injected by the
        FileSource SMT flows through ksqlDB to the HTTP sink and ultimately to
        the storage service upload call.
        """
        namespace_id = str(uuid.uuid4())
        group_id = str(uuid.uuid4())

        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)
        _produce_file_event(
            bootstrap_server,
            path="/watch/enterprise/doc.txt",
            display_name="doc.txt",
            namespace="e2e-ns",
            namespace_id=namespace_id,
            org_name="e2e-org",
            group_id=group_id,
        )

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC, start)
        assert record["group_id"] == group_id
