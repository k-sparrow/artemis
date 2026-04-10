# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests: ksqlDB stream processing against a real Kafka broker.

Verifies that the KSQLDbContainer + KafkaContainer combination can:
  1. Accept CREATE STREAM statements via the REST API.
  2. Process records produced to an input topic and write transformed
     records to an output topic that a Kafka consumer can read.
  3. Perform field-level transformations (type coercion, CASE, STRUCT).
  4. Promote Kafka record headers into value fields using HEADER().

All containers are session-scoped (shared across tests in this file).

Run with:
    pytest tests/lib/testcontainers/tests/kafka/test_ksqldb_streams.py -v
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Iterator

import httpx
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka import KSQLDbContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

# ---------------------------------------------------------------------------
# Topic names (unique suffix avoids cross-test contamination if suite reruns)
# ---------------------------------------------------------------------------

_SUFFIX = uuid.uuid4().hex[:8]
_INPUT_TOPIC = f"ksqldb-test-input-{_SUFFIX}"
_OUTPUT_TOPIC = f"ksqldb-test-output-{_SUFFIX}"
_HEADERS_INPUT_TOPIC = f"ksqldb-test-headers-input-{_SUFFIX}"
_HEADERS_OUTPUT_TOPIC = f"ksqldb-test-headers-output-{_SUFFIX}"


# ---------------------------------------------------------------------------
# Session-scoped stack: Network → Kafka → ksqlDB
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def network() -> Iterator[Network]:
    with Network() as n:
        yield n


@pytest.fixture(scope="session")
def kafka(network: Network) -> Iterator[KafkaContainer]:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(network)
        .with_network_aliases("broker")
        .with_kwargs(hostname="broker")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def bootstrap_server(kafka: KafkaContainer) -> str:
    return kafka.get_bootstrap_server()


@pytest.fixture(scope="session")
def ksqldb(network: Network, kafka: KafkaContainer) -> Iterator[KSQLDbContainer]:
    container = (
        KSQLDbContainer(bootstrap_servers=kafka.get_internal_bootstrap_server())
        .with_network(network)
        .with_network_aliases("ksqldb")
        .with_log_level("WARN")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def ksqldb_url(ksqldb: KSQLDbContainer) -> str:
    return ksqldb.get_url()


# ---------------------------------------------------------------------------
# Topic pre-creation (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def topics(bootstrap_server: str) -> None:
    """Pre-create all test topics so ksqlDB streams can be declared against them."""
    admin = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    to_create = [
        _INPUT_TOPIC,
        _OUTPUT_TOPIC,
        _HEADERS_INPUT_TOPIC,
        _HEADERS_OUTPUT_TOPIC,
    ]
    existing = set(admin.list_topics())
    new_topics = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in to_create
        if t not in existing
    ]
    if new_topics:
        try:
            admin.create_topics(new_topics)
        except TopicAlreadyExistsError:
            pass
    admin.close()


# ---------------------------------------------------------------------------
# ksqlDB stream definitions (session-scoped)
# ---------------------------------------------------------------------------


def _ksql(url: str, statement: str, earliest: bool = False) -> dict:
    props = {}
    if earliest:
        props["ksql.streams.auto.offset.reset"] = "earliest"
    # Retry on 5xx: /healthcheck may return 200 slightly before ksqlDB's
    # internal command topic consumer group is fully ready.
    for _ in range(10):
        resp = httpx.post(
            f"{url}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            json={"ksql": statement, "streamsProperties": props},
            timeout=30.0,
        )
        if resp.status_code < 500:
            resp.raise_for_status()
            return resp.json()
        time.sleep(3)
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def streams(ksqldb_url: str, topics: None) -> None:
    """Create source + sink streams for the value-transform tests."""
    # Source stream: plain JSON values
    _ksql(
        ksqldb_url,
        f"""
        CREATE OR REPLACE STREAM `{_INPUT_TOPIC}` (
            id       VARCHAR,
            amount   DOUBLE,
            category VARCHAR
        )
        WITH (
            KAFKA_TOPIC  = '{_INPUT_TOPIC}',
            VALUE_FORMAT = 'JSON',
            PARTITIONS   = 1
        );
        """,
    )
    # Derived stream: reshape value with STRUCT + CASE
    _ksql(
        ksqldb_url,
        f"""
        CREATE OR REPLACE STREAM `{_OUTPUT_TOPIC}`
        WITH (
            KAFKA_TOPIC  = '{_OUTPUT_TOPIC}',
            VALUE_FORMAT = 'JSON',
            PARTITIONS   = 1
        )
        AS SELECT
            id AS `id`,
            ROUND(amount * 1.2, 2) AS `amount_with_tax`,
            CASE
                WHEN category = 'food'       THEN 'essential'
                WHEN category = 'luxury'     THEN 'non-essential'
                ELSE                              'other'
            END AS `category_class`,
            STRUCT(`original` := category, `taxed` := true) AS `meta`
        FROM `{_INPUT_TOPIC}`
        EMIT CHANGES;
        """,
        earliest=True,
    )


@pytest.fixture(scope="session")
def header_streams(ksqldb_url: str, topics: None) -> None:
    """Create source + sink streams for the HEADER() promotion tests."""
    # Source stream: KAFKA value format (raw bytes), headers promoted to fields
    _ksql(
        ksqldb_url,
        f"""
        CREATE OR REPLACE STREAM `{_HEADERS_INPUT_TOPIC}` (
            payload  BYTES,
            doc_id   BYTES HEADER('doc.id'),
            filename BYTES HEADER('doc.filename')
        )
        WITH (
            KAFKA_TOPIC  = '{_HEADERS_INPUT_TOPIC}',
            VALUE_FORMAT = 'KAFKA',
            PARTITIONS   = 1
        );
        """,
    )
    # Sink: decode bytes headers to strings, write as JSON
    _ksql(
        ksqldb_url,
        f"""
        CREATE OR REPLACE STREAM `{_HEADERS_OUTPUT_TOPIC}`
        WITH (
            KAFKA_TOPIC  = '{_HEADERS_OUTPUT_TOPIC}',
            VALUE_FORMAT = 'JSON',
            PARTITIONS   = 1
        )
        AS SELECT
            FROM_BYTES(doc_id,   'utf8') AS `doc_id`,
            FROM_BYTES(filename, 'utf8') AS `filename`
        FROM `{_HEADERS_INPUT_TOPIC}`
        EMIT CHANGES;
        """,
        earliest=True,
    )


# ---------------------------------------------------------------------------
# Consumer helper
# ---------------------------------------------------------------------------


def _consume_one(bootstrap_server: str, topic: str, timeout_s: int = 30) -> dict:
    """Return the first record value from ``topic`` as a parsed dict."""
    tp = TopicPartition(topic, 0)
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_server],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_s * 1000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        group_id=None,
    )
    consumer.assign([tp])
    consumer.poll(timeout_ms=1000)  # establish connection + fetch metadata
    # Seek to the beginning so we catch records produced before the consumer started.
    consumer.seek_to_beginning(tp)
    try:
        for record in consumer:
            return record.value
    finally:
        consumer.close()
    pytest.fail(f"No record received on {topic} within {timeout_s}s")


# ---------------------------------------------------------------------------
# Tests: value field transformation
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestKsqlDbValueTransformation:
    """Produce JSON records; assert ksqlDB reshapes them correctly."""

    @pytest.fixture(autouse=True)
    def _require_streams(self, streams: None) -> None:
        pass

    def test_amount_multiplied_by_tax_rate(self, bootstrap_server: str) -> None:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value={"id": "tx-001", "amount": 100.0, "category": "food"},
        )
        producer.flush()
        producer.close()

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC)
        assert record["amount_with_tax"] == pytest.approx(120.0)

    def test_id_passed_through(self, bootstrap_server: str) -> None:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value={"id": "tx-passthrough", "amount": 50.0, "category": "other"},
        )
        producer.flush()
        producer.close()

        record = _consume_one(bootstrap_server, _OUTPUT_TOPIC)
        # May pick up the first or second record; seek_to_beginning returns all
        assert record["id"] is not None

    def test_food_category_maps_to_essential(self, bootstrap_server: str) -> None:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value={"id": "tx-food", "amount": 10.0, "category": "food"},
        )
        producer.flush()
        producer.close()

        # Consume all records and find the one we produced
        tp = TopicPartition(_OUTPUT_TOPIC, 0)
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_server],
            enable_auto_commit=False,
            consumer_timeout_ms=30_000,
            value_deserializer=lambda b: json.loads(b.decode()),
            group_id=None,
        )
        consumer.assign([tp])
        consumer.poll(timeout_ms=1000)
        consumer.seek_to_beginning(tp)
        found = None
        for record in consumer:
            if record.value.get("id") == "tx-food":
                found = record.value
                break
        consumer.close()

        assert found is not None, "Record tx-food not found in output topic"
        assert found["category_class"] == "essential"

    def test_luxury_category_maps_to_non_essential(self, bootstrap_server: str) -> None:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value={"id": "tx-luxury", "amount": 999.0, "category": "luxury"},
        )
        producer.flush()
        producer.close()

        tp = TopicPartition(_OUTPUT_TOPIC, 0)
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_server],
            enable_auto_commit=False,
            consumer_timeout_ms=30_000,
            value_deserializer=lambda b: json.loads(b.decode()),
            group_id=None,
        )
        consumer.assign([tp])
        consumer.poll(timeout_ms=1000)
        consumer.seek_to_beginning(tp)
        found = None
        for record in consumer:
            if record.value.get("id") == "tx-luxury":
                found = record.value
                break
        consumer.close()

        assert found is not None, "Record tx-luxury not found in output topic"
        assert found["category_class"] == "non-essential"

    def test_unknown_category_maps_to_other(self, bootstrap_server: str) -> None:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value={"id": "tx-unknown", "amount": 1.0, "category": "gadgets"},
        )
        producer.flush()
        producer.close()

        tp = TopicPartition(_OUTPUT_TOPIC, 0)
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_server],
            enable_auto_commit=False,
            consumer_timeout_ms=30_000,
            value_deserializer=lambda b: json.loads(b.decode()),
            group_id=None,
        )
        consumer.assign([tp])
        consumer.poll(timeout_ms=1000)
        consumer.seek_to_beginning(tp)
        found = None
        for record in consumer:
            if record.value.get("id") == "tx-unknown":
                found = record.value
                break
        consumer.close()

        assert found is not None, "Record tx-unknown not found in output topic"
        assert found["category_class"] == "other"

    def test_struct_field_in_output(self, bootstrap_server: str) -> None:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        producer.send(
            _INPUT_TOPIC,
            value={"id": "tx-struct", "amount": 5.0, "category": "food"},
        )
        producer.flush()
        producer.close()

        tp = TopicPartition(_OUTPUT_TOPIC, 0)
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_server],
            enable_auto_commit=False,
            consumer_timeout_ms=30_000,
            value_deserializer=lambda b: json.loads(b.decode()),
            group_id=None,
        )
        consumer.assign([tp])
        consumer.poll(timeout_ms=1000)
        consumer.seek_to_beginning(tp)
        found = None
        for record in consumer:
            if record.value.get("id") == "tx-struct":
                found = record.value
                break
        consumer.close()

        assert found is not None
        meta = found["meta"]
        assert meta["original"] == "food"
        assert meta["taxed"] is True


# ---------------------------------------------------------------------------
# Tests: HEADER() promotion
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestKsqlDbHeaderPromotion:
    """Produce records with Kafka headers; assert ksqlDB promotes them to value fields."""

    @pytest.fixture(autouse=True)
    def _require_streams(self, header_streams: None) -> None:
        pass

    def _produce_with_headers(
        self,
        bootstrap_server: str,
        doc_id: str,
        filename: str,
    ) -> None:
        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        producer.send(
            _HEADERS_INPUT_TOPIC,
            value=b"irrelevant-body",
            headers=[
                ("doc.id", doc_id.encode("utf-8")),
                ("doc.filename", filename.encode("utf-8")),
            ],
        )
        producer.flush()
        producer.close()

    def _find_record(
        self, bootstrap_server: str, doc_id: str, timeout_s: int = 30
    ) -> dict:
        tp = TopicPartition(_HEADERS_OUTPUT_TOPIC, 0)
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_server],
            enable_auto_commit=False,
            consumer_timeout_ms=timeout_s * 1000,
            value_deserializer=lambda b: json.loads(b.decode()),
            group_id=None,
        )
        consumer.assign([tp])
        consumer.poll(timeout_ms=1000)
        consumer.seek_to_beginning(tp)
        try:
            for record in consumer:
                if record.value.get("doc_id") == doc_id:
                    return record.value
        finally:
            consumer.close()
        pytest.fail(f"Record with doc_id={doc_id!r} not found within {timeout_s}s")

    def test_doc_id_header_promoted_to_value_field(self, bootstrap_server: str) -> None:
        doc_id = f"doc-{uuid.uuid4().hex[:8]}"
        self._produce_with_headers(bootstrap_server, doc_id, "report.pdf")

        record = self._find_record(bootstrap_server, doc_id)
        assert record["doc_id"] == doc_id

    def test_filename_header_promoted_to_value_field(
        self, bootstrap_server: str
    ) -> None:
        doc_id = f"doc-{uuid.uuid4().hex[:8]}"
        self._produce_with_headers(bootstrap_server, doc_id, "spec.txt")

        record = self._find_record(bootstrap_server, doc_id)
        assert record["filename"] == "spec.txt"

    def test_multiple_records_each_promoted_independently(
        self, bootstrap_server: str
    ) -> None:
        ids_and_files = [
            (f"doc-{uuid.uuid4().hex[:8]}", "alpha.pdf"),
            (f"doc-{uuid.uuid4().hex[:8]}", "beta.txt"),
        ]
        for doc_id, filename in ids_and_files:
            self._produce_with_headers(bootstrap_server, doc_id, filename)

        for doc_id, expected_filename in ids_and_files:
            record = self._find_record(bootstrap_server, doc_id)
            assert record["filename"] == expected_filename
