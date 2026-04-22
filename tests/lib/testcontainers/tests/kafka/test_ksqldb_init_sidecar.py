# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests: ksqlDB init-sidecar pattern.

Verifies the general principle that a one-shot CLI container can initialise
a ksqlDB server from a ``.ksql`` file, and that the resulting streams
correctly process records end-to-end.

Mirrors ``test_ksqldb_streams.py`` but uses the init-sidecar approach
(``ksql --file``) instead of direct REST API calls to create the streams.
This exercises the same mechanism used by ``artemis/ksqldb-init`` and
``artemis/ksqldb-enterprise-init`` in production.

Stack:
    Network → Kafka (KRaft) → KSQLDbContainer
                            → cp-ksqldb-cli (one-shot, mounts test .ksql file)

The one-shot container polls ``/healthcheck``, runs ``/bin/ksql --file``
against the server, then exits 0.  Stream creation is verified by producing
records to the input topic and consuming from the output topic.

Run with:
    pytest tests/lib/testcontainers/tests/kafka/test_ksqldb_init_sidecar.py -v

Note: tagged ``local`` because the test mounts a host temp file into the
sidecar container.  This disables the Bazel sandbox so Docker can reach
the real ``/tmp`` path.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Iterator

import httpx
import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from tests.lib.testcontainers.kafka import KSQLDbContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

# ---------------------------------------------------------------------------
# Bridge fixtures: conftest.py exposes container objects; tests need URLs.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bootstrap_server(kafka_broker: KafkaContainer) -> str:
    return kafka_broker.get_bootstrap_server()


@pytest.fixture(scope="session")
def ksqldb_url(ksqldb_server: KSQLDbContainer) -> str:
    return ksqldb_server.get_url()


# ---------------------------------------------------------------------------
# Test-run-unique topic / stream names to avoid cross-run contamination
# ---------------------------------------------------------------------------

_SUFFIX = uuid.uuid4().hex[:8]
_INPUT_TOPIC = f"sidecar-test-input-{_SUFFIX}"
_OUTPUT_TOPIC = f"sidecar-test-output-{_SUFFIX}"
_ROUTED_TOPIC = f"sidecar-test-routed-{_SUFFIX}"
_SOURCE_STREAM = f"sidecar-test-source-{_SUFFIX}"
_DERIVED_STREAM = f"sidecar-test-derived-{_SUFFIX}"
_ROUTED_STREAM = f"sidecar-test-routed-{_SUFFIX}"

# ---------------------------------------------------------------------------
# .ksql content: header promotion + FROM_BYTES (mirrors the real init scripts)
# ---------------------------------------------------------------------------
#
# Source stream: VALUE_FORMAT=KAFKA (raw bytes) with two promoted Kafka headers.
# Derived stream: decodes the header bytes to UTF-8 strings.
# This mirrors what artemis/ksqldb-enterprise-init does for filesystem events.

_TEST_KSQL = f"""
SET 'auto.offset.reset' = 'earliest';

-- Source: raw bytes value, two fields promoted from Kafka record headers.
CREATE OR REPLACE STREAM `{_SOURCE_STREAM}` (
    `payload`  BYTES,
    `item_id`  BYTES HEADER('x.item.id'),
    `category` BYTES HEADER('x.item.category')
)
WITH (
    KAFKA_TOPIC  = '{_INPUT_TOPIC}',
    VALUE_FORMAT = 'KAFKA',
    PARTITIONS   = 1
);

-- Derived: decode header bytes to UTF-8 strings, emit as JSON.
CREATE OR REPLACE STREAM `{_DERIVED_STREAM}`
WITH (
    KAFKA_TOPIC  = '{_OUTPUT_TOPIC}',
    VALUE_FORMAT = 'JSON',
    PARTITIONS   = 1
)
AS SELECT
    FROM_BYTES(`item_id`,  'utf8') AS `item_id`,
    FROM_BYTES(`category`, 'utf8') AS `category`
FROM `{_SOURCE_STREAM}`
EMIT CHANGES;

-- Routed: re-partition by item_id so it becomes the Kafka record KEY.
-- Mirrors artemis-ingestion-celery-tasks-routed (PARTITION BY STRUCT task_id).
-- PARTITION BY STRUCT produces a named key {{ "item_id": "<value>" }} directly,
-- so HeaderFrom$Key can extract the field without needing HoistField$Key.
-- ksqlDB strips the PARTITION BY expression from the VALUE.
CREATE OR REPLACE STREAM `{_ROUTED_STREAM}`
WITH (
    KAFKA_TOPIC  = '{_ROUTED_TOPIC}',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT   = 'JSON',
    PARTITIONS   = 1
)
AS SELECT
    STRUCT(`item_id` := `item_id`) as `ikey`,
    `category`
FROM `{_DERIVED_STREAM}`
PARTITION BY STRUCT(`item_id` := `item_id`)
EMIT CHANGES;
"""

# cp-ksqldb-cli image: same major version as KSQLDbContainer.DEFAULT_IMAGE.
# Must have /bin/ksql and curl available (stock cp-ksqldb-cli does).
_CLI_IMAGE = f"confluentinc/cp-ksqldb-cli:{KSQLDbContainer.DEFAULT_IMAGE.split(':')[1]}"

_KSQLDB_ALIAS = "ksqldb-server"  # network alias set in conftest.py

# Inline entrypoint: poll healthcheck, then run ksql --file, then exit 0.
_INIT_CMD = (
    "set -e; "
    f"until [ \"$(curl -s -o /dev/null -w '%{{http_code}}' "
    f"http://{_KSQLDB_ALIAS}:{KSQLDbContainer.HTTP_PORT}/healthcheck)\" = '200' ]; "
    "do echo 'waiting for ksqlDB...'; sleep 2; done; "
    "/bin/ksql --file /ksql/init.ksql "
    f"-- http://{_KSQLDB_ALIAS}:{KSQLDbContainer.HTTP_PORT}; "
    "echo '✓ Done.'"
)


# ---------------------------------------------------------------------------
# Session-scoped: write the .ksql file to a real /tmp path so Docker can
# bind-mount it (Bazel sandbox is disabled by the 'local' tag).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ksql_file(request: pytest.FixtureRequest) -> Path:
    """Write the test .ksql content to a real host path for Docker to mount."""
    d = Path(tempfile.mkdtemp(prefix="ksqldb-sidecar-test-"))
    request.addfinalizer(lambda: shutil.rmtree(d, ignore_errors=True))
    f = d / "init.ksql"
    f.write_text(_TEST_KSQL, encoding="utf-8")
    return f


# ---------------------------------------------------------------------------
# Topic pre-creation (ksqlDB requires the topic to exist before CSAS starts)
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
# One-shot init sidecar (session-scoped — streams persist for all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def init_sidecar(
    kafka_network: Network,
    ksqldb_server: KSQLDbContainer,
    ksql_file: Path,
    topics: None,
) -> int:
    """Run the cp-ksqldb-cli one-shot container and return its exit code."""
    container = (
        DockerContainer(_CLI_IMAGE)
        .with_network(kafka_network)
        .with_volume_mapping(str(ksql_file), "/ksql/init.ksql", "ro")
        .with_kwargs(entrypoint=["bash", "-c", _INIT_CMD])
    )
    container.waiting_for(LogMessageWaitStrategy(r"✓ Done\.").with_startup_timeout(120))
    container.start()
    result = container.get_wrapped_container().wait(timeout=30)
    container.stop()
    return result["StatusCode"]


# ---------------------------------------------------------------------------
# Consumer helpers (end_offsets+seek pattern — avoids stale-message races)
# ---------------------------------------------------------------------------


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
) -> Iterator[dict]:
    """Yield decoded JSON records from *topic* starting at *start_offset*."""
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
        yield from consumer
    finally:
        consumer.close()


def _find_record(
    bootstrap_server: str,
    topic: str,
    start_offset: int,
    item_id: str,
    timeout_s: int = 30,
) -> dict:
    for record in _consume_from(bootstrap_server, topic, start_offset, timeout_s):
        if record.value.get("item_id") == item_id:
            return record.value
    pytest.fail(
        f"No record with item_id={item_id!r} found on {topic} within {timeout_s}s"
    )


def _find_routed_record(
    bootstrap_server: str,
    topic: str,
    start_offset: int,
    item_id: str,
    timeout_s: int = 30,
) -> tuple[bytes | None, dict]:
    """Return (raw_key_bytes, value) for the first routed record matching item_id.

    The routed stream uses VALUE_FORMAT=JSON so we can match on the value,
    but the KEY is raw bytes that we return for the caller to inspect.
    """
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
            if record.value.get("category") is not None:
                # PARTITION BY STRUCT key is {"item_id": "<value>"}
                raw_key = record.key
                if raw_key is not None:
                    key_struct = json.loads(raw_key.decode())
                    if (
                        isinstance(key_struct, dict)
                        and key_struct.get("item_id") == item_id
                    ):
                        return raw_key, record.value
    finally:
        consumer.close()
    pytest.fail(
        f"No routed record with item_id={item_id!r} found on {topic} within {timeout_s}s"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestKsqlDbInitSidecar:
    """The init sidecar creates working streams; records flow end-to-end."""

    def test_sidecar_exits_with_code_zero(self, init_sidecar: int) -> None:
        assert init_sidecar == 0

    def test_source_stream_discoverable(
        self, ksqldb_url: str, init_sidecar: int
    ) -> None:
        resp = httpx.post(
            f"{ksqldb_url}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            json={"ksql": "SHOW STREAMS;"},
            timeout=10.0,
        )
        resp.raise_for_status()
        names = {s["name"] for s in resp.json()[0]["streams"]}
        assert _SOURCE_STREAM in names

    def test_derived_stream_discoverable(
        self, ksqldb_url: str, init_sidecar: int
    ) -> None:
        resp = httpx.post(
            f"{ksqldb_url}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            json={"ksql": "SHOW STREAMS;"},
            timeout=10.0,
        )
        resp.raise_for_status()
        names = {s["name"] for s in resp.json()[0]["streams"]}
        assert _DERIVED_STREAM in names

    def test_item_id_header_promoted_to_value(
        self, bootstrap_server: str, init_sidecar: int
    ) -> None:
        item_id = f"item-{uuid.uuid4().hex[:8]}"
        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        producer.send(
            _INPUT_TOPIC,
            value=b"",
            headers=[
                ("x.item.id", item_id.encode()),
                ("x.item.category", b"electronics"),
            ],
        )
        producer.flush()
        producer.close()

        record = _find_record(bootstrap_server, _OUTPUT_TOPIC, start, item_id)
        assert record["item_id"] == item_id

    def test_category_header_promoted_to_value(
        self, bootstrap_server: str, init_sidecar: int
    ) -> None:
        item_id = f"item-{uuid.uuid4().hex[:8]}"
        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        producer.send(
            _INPUT_TOPIC,
            value=b"",
            headers=[
                ("x.item.id", item_id.encode()),
                ("x.item.category", b"books"),
            ],
        )
        producer.flush()
        producer.close()

        record = _find_record(bootstrap_server, _OUTPUT_TOPIC, start, item_id)
        assert record["category"] == "books"

    def test_routed_stream_discoverable(
        self, ksqldb_url: str, init_sidecar: int
    ) -> None:
        resp = httpx.post(
            f"{ksqldb_url}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            json={"ksql": "SHOW STREAMS;"},
            timeout=10.0,
        )
        resp.raise_for_status()
        names = {s["name"] for s in resp.json()[0]["streams"]}
        assert _ROUTED_STREAM in names

    def test_routed_item_id_is_record_key(
        self, bootstrap_server: str, init_sidecar: int
    ) -> None:
        """PARTITION BY STRUCT → key is {"item_id": "<value>"}, not a bare string.

        HeaderFrom$Key can extract the named field directly — no HoistField$Key needed.
        """
        item_id = f"item-{uuid.uuid4().hex[:8]}"
        start = _end_offset(bootstrap_server, _ROUTED_TOPIC)

        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        producer.send(
            _INPUT_TOPIC,
            value=b"",
            headers=[
                ("x.item.id", item_id.encode()),
                ("x.item.category", b"electronics"),
            ],
        )
        producer.flush()
        producer.close()

        raw_key, _ = _find_routed_record(
            bootstrap_server, _ROUTED_TOPIC, start, item_id
        )
        assert raw_key is not None
        # PARTITION BY STRUCT produces a named struct key, not a bare JSON string
        assert json.loads(raw_key.decode()) == {"item_id": item_id}

    def test_routed_item_id_absent_from_value(
        self, bootstrap_server: str, init_sidecar: int
    ) -> None:
        """PARTITION BY strips item_id from the VALUE — it lives in the KEY only."""
        item_id = f"item-{uuid.uuid4().hex[:8]}"
        start = _end_offset(bootstrap_server, _ROUTED_TOPIC)

        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        producer.send(
            _INPUT_TOPIC,
            value=b"",
            headers=[
                ("x.item.id", item_id.encode()),
                ("x.item.category", b"books"),
            ],
        )
        producer.flush()
        producer.close()

        _, value = _find_routed_record(bootstrap_server, _ROUTED_TOPIC, start, item_id)
        assert "item_id" not in value

    def test_routed_value_retains_other_fields(
        self, bootstrap_server: str, init_sidecar: int
    ) -> None:
        """Non-key fields survive in the VALUE after re-partitioning."""
        item_id = f"item-{uuid.uuid4().hex[:8]}"
        start = _end_offset(bootstrap_server, _ROUTED_TOPIC)

        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        producer.send(
            _INPUT_TOPIC,
            value=b"",
            headers=[
                ("x.item.id", item_id.encode()),
                ("x.item.category", b"luxury"),
            ],
        )
        producer.flush()
        producer.close()

        _, value = _find_routed_record(bootstrap_server, _ROUTED_TOPIC, start, item_id)
        assert value["category"] == "luxury"

    def test_multiple_records_each_transformed_independently(
        self, bootstrap_server: str, init_sidecar: int
    ) -> None:
        items = [
            (f"item-{uuid.uuid4().hex[:8]}", cat)
            for cat in ("food", "luxury", "electronics")
        ]
        start = _end_offset(bootstrap_server, _OUTPUT_TOPIC)

        producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        for item_id, category in items:
            producer.send(
                _INPUT_TOPIC,
                value=b"",
                headers=[
                    ("x.item.id", item_id.encode()),
                    ("x.item.category", category.encode()),
                ],
            )
        producer.flush()
        producer.close()

        found: dict[str, str] = {}
        expected_ids = {item_id for item_id, _ in items}
        for record in _consume_from(
            bootstrap_server, _OUTPUT_TOPIC, start, timeout_s=30
        ):
            rid = record.value.get("item_id")
            if rid in expected_ids:
                found[rid] = record.value["category"]
            if len(found) == len(items):
                break

        for item_id, expected_category in items:
            assert found.get(item_id) == expected_category, (
                f"item_id={item_id!r}: expected category={expected_category!r}, "
                f"got {found.get(item_id)!r}"
            )
