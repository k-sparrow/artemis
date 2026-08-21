"""KafkaConnect container tests for the artemis/cp-kafka-connect image.

Verifies that the custom-built KafkaConnect image:
  1. Ships the CamelFileSource and CamelFileWatchSource connector plugins.
  2. Can deploy a FileSource connector using the production template and reach RUNNING.
  3. Produces messages with the correct headers (artemis.namespace, artemis.org_name,
     CamelHeader.CamelFileAbsolutePath).
  4. Does not duplicate-ingest files from a tree larger than Camel's default
     idempotent repository capacity (TestFilesourceConnectorIdempotency).

Pre-requisite:
    artemis/cp-kafka-connect:latest must be built.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Iterator

import httpx
import pytest
from kafka import KafkaConsumer, TopicPartition
from kafka_connect import KafkaConnect

from src.backend.enterprise.data_sources.api.sources.templates import (
    render_filesystem_connector,
)
from tests.lib.polling import poll_until, wait_for_kc_connector

_TOPIC = "artemis.datasource.filesystem"
_NAMESPACE = "test-ns"
_NAMESPACE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_CONNECTOR_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_ORG_NAME = "test-org"
_OWNER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _kc_client(kafka_connect_url: str) -> KafkaConnect:
    return KafkaConnect(url=kafka_connect_url)


def _list_all_plugins(kafka_connect_url: str) -> set[str]:
    """Return all plugin classes including SMTs and converters.

    The kafka-connect-py client calls GET /connector-plugins without
    connectorsOnly=false, which omits SMTs. We call the endpoint directly.
    """
    resp = httpx.get(
        f"{kafka_connect_url}/connector-plugins",
        params={"connectorsOnly": "false"},
        timeout=10.0,
    )
    resp.raise_for_status()
    return {p["class"] for p in resp.json()}


def _wait_connector_running(client: KafkaConnect, name: str, timeout: int = 60) -> None:
    from requests.exceptions import HTTPError

    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            last = client.get_connector_status(name)
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                time.sleep(2)
                continue
            raise
        tasks = last.get("tasks", [])
        if (
            last.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t["state"] == "RUNNING" for t in tasks)
        ):
            return
        time.sleep(2)
    pytest.fail(
        f"Connector {name!r} did not reach RUNNING within {timeout}s. "
        f"Last status: {last}"
    )


@pytest.mark.parametrize(
    "plugin_class",
    [
        # ── Connector plugins ────────────────────────────────────────────────
        pytest.param(
            "org.apache.camel.kafkaconnector.file.CamelFileSourceConnector",
            id="CamelFileSource",
        ),
        pytest.param(
            "org.apache.camel.kafkaconnector.filewatchsource.CamelFilewatchsourceSourceConnector",  # noqa: E501
            id="CamelFileWatchSource",
        ),
        # ── SMTs used by the FileSource connector template ───────────────────
        # Key transforms
        pytest.param(
            "org.apache.kafka.connect.transforms.HoistField$Key",
            id="HoistFieldKey",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.InsertField$Key",
            id="InsertFieldKey",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.ReplaceField$Key",
            id="ReplaceFieldKey",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.ExtractField$Key",
            id="ExtractFieldKey",
        ),
        # Header manipulation
        pytest.param(
            "org.apache.kafka.connect.transforms.DropHeaders",
            id="DropHeaders",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.InsertHeader",
            id="InsertHeader",
        ),
    ],
)
def test_required_plugin_installed(kafka_connect_url: str, plugin_class: str) -> None:
    """artemis/cp-kafka-connect must ship all required connector plugins and SMTs."""
    classes = _list_all_plugins(kafka_connect_url)
    assert (
        plugin_class in classes
    ), f"{plugin_class} not found. Installed: {sorted(classes)}"


class TestFilesourceConnector:
    """Deploy our production FileSource template and verify runtime behaviour."""

    @pytest.fixture(autouse=True)
    def connector(self, kafka_connect_url: str) -> Iterator[KafkaConnect]:
        """Deploy a uniquely-named connector per test to avoid stale offsets."""
        client = _kc_client(kafka_connect_url)
        connector_name = f"test-filesource-{uuid.uuid4().hex[:8]}"
        config = render_filesystem_connector(
            connector_name=connector_name,
            connector_id=_CONNECTOR_ID,
            watch_path="/watch",
            namespace=_NAMESPACE,
            namespace_id=_NAMESPACE_ID,
            org_name=_ORG_NAME,
            owner_id=_OWNER_ID,
        )
        client.create_connector(config)
        self._connector_name = connector_name
        yield client
        try:
            client.delete_connector(connector_name)
        except Exception:
            pass

    def _consume_messages(self, kafka_bootstrap_server: str) -> list:
        tp = TopicPartition(_TOPIC, 0)
        c = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_server,
            enable_auto_commit=False,
            consumer_timeout_ms=30_000,
            group_id=None,
        )
        c.assign([tp])
        c.seek(tp, 0)
        msgs = list(c)
        c.close()
        return msgs

    def test_connector_reaches_running_state(self, connector: KafkaConnect) -> None:
        _wait_connector_running(connector, self._connector_name)

    @pytest.mark.parametrize(
        "header,expected_value",
        [
            pytest.param("artemis.namespace", _NAMESPACE, id="namespace"),
            pytest.param("artemis.namespace_id", _NAMESPACE_ID, id="namespace_id"),
            pytest.param("artemis.org_name", _ORG_NAME, id="org_name"),
            pytest.param("artemis.group_id", _CONNECTOR_ID, id="group_id"),
            pytest.param("artemis.owner_id", _OWNER_ID, id="owner_id"),
        ],
    )
    def test_static_header_present_and_correct(
        self,
        connector: KafkaConnect,
        kafka_bootstrap_server: str,
        header: str,
        expected_value: str,
    ) -> None:
        _wait_connector_running(connector, self._connector_name)
        msgs = self._consume_messages(kafka_bootstrap_server)
        assert len(msgs) >= 1, "Expected ≥1 message from pre-seeded files"
        for msg in msgs:
            headers = dict(msg.headers)
            assert header in headers, f"{header!r} header missing. headers={headers}"
            assert headers[header].decode() == expected_value

    def test_file_path_header_present_and_correct(
        self, connector: KafkaConnect, kafka_bootstrap_server: str
    ) -> None:
        _wait_connector_running(connector, self._connector_name)
        msgs = self._consume_messages(kafka_bootstrap_server)
        assert len(msgs) >= 1
        for msg in msgs:
            headers = dict(msg.headers)
            assert (
                "CamelHeader.CamelFileAbsolutePath" in headers
            ), f"CamelHeader.CamelFileAbsolutePath missing. headers={headers}"
            path = headers["CamelHeader.CamelFileAbsolutePath"].decode()
            assert path.startswith(
                "/watch/"
            ), f"Expected path under /watch/, got: {path}"


class TestFilesourceConnectorIdempotency:
    """Regression test for the eviction-driven duplicate-ingestion bug.

    Camel's default MemoryIdempotentRepository holds only 1000 entries with
    silent LRU eviction, and noop=true (mandatory for our RO-mounted trees)
    means it's the *only* de-dup mechanism available — no physical marker
    backs it up. Once a watched tree exceeds that capacity, evicted files
    look "new" again on the very next poll and get reprocessed forever,
    without the connector task ever failing or logging a warning (verified
    manually: 100k+ duplicate records within minutes against a 1200-file
    tree). render_filesystem_connector fixes this by sizing the repository
    well above any expected tree (see templates.py's idempotent_cache_size).

    This suite watches a tree sized to exceed Camel's own undersized default
    (1000) but well under the shipped production default (50000), so it
    fails if that sizing ever regresses.
    """

    _FILE_COUNT = 1200

    @pytest.fixture
    def watch_dir(self) -> Iterator[Path]:
        """Overrides the module-level watch_dir fixture with a large tree.

        Uses tempfile.mkdtemp() rather than pytest's tmp_path for the same
        reason as the default watch_dir fixture: Docker must be able to see
        the real host path, which Bazel's sandboxed /tmp is not.
        """
        import shutil
        import tempfile

        d = Path(tempfile.mkdtemp(prefix="kc-test-idempotency-watch-"))
        for i in range(self._FILE_COUNT):
            (d / f"doc_{i:04d}.txt").write_text(f"content {i}")
        yield d
        shutil.rmtree(d, ignore_errors=True)

    def _record_count(self, kafka_bootstrap_server: str) -> int:
        tp = TopicPartition(_TOPIC, 0)
        c = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_server,
            enable_auto_commit=False,
            consumer_timeout_ms=2_000,
            group_id=None,
        )
        c.assign([tp])
        c.seek(tp, 0)
        count = len(list(c))
        c.close()
        return count

    def test_no_duplicate_records_across_multiple_polls(
        self, kafka_connect_url: str, kafka_bootstrap_server: str, kafka_topic: str
    ) -> None:
        # Pre-create the output topic. Otherwise the connector's very first
        # sends can race topic auto-creation, fail their exchange, and get
        # legitimately reprocessed once on the next poll (GenericFileOnCompletion's
        # rollback path) — a real but separate behaviour from the eviction bug
        # this test targets, and it would make the count assertions flaky.
        client = _kc_client(kafka_connect_url)
        connector_name = f"test-filesource-idempotency-{uuid.uuid4().hex[:8]}"
        config = render_filesystem_connector(
            connector_name=connector_name,
            connector_id=_CONNECTOR_ID,
            watch_path="/watch",
            namespace=_NAMESPACE,
            namespace_id=_NAMESPACE_ID,
            org_name=_ORG_NAME,
            owner_id=_OWNER_ID,
            # Production default is 10 minutes; only the poll rate is
            # shortened here to keep the test fast. idempotent_cache_size is
            # left at its real production default — that's what's under test.
            poll_delay_ms=1000,
        )
        client.create_connector(config)
        try:
            wait_for_kc_connector(kafka_connect_url, connector_name, timeout=60)

            def _reached_full_count() -> bool:
                return self._record_count(kafka_bootstrap_server) >= self._FILE_COUNT

            poll_until(_reached_full_count, timeout=120, interval=3.0)

            # Kafka Connect source connectors are at-least-once by default
            # here (no exactly-once support configured), so a handful of
            # duplicates from in-flight redelivery during the initial burst
            # scan is expected and not what this test targets. Sanity-check
            # it's a handful, not a flood.
            count_after_scan = self._record_count(kafka_bootstrap_server)
            assert count_after_scan < self._FILE_COUNT * 1.05, (
                f"Expected roughly {self._FILE_COUNT} records after the initial "
                f"scan, got {count_after_scan} — more than ordinary at-least-once "
                "redelivery would explain; looks like eviction-driven reprocessing."
            )

            # The eviction bug's actual signature is *unbounded, continuous*
            # growth on every subsequent poll (verified manually: tens of
            # thousands of duplicates within seconds, never plateauing) — so
            # the real regression check is that the count stabilises once the
            # initial burst settles, not that it hits an exact number.
            time.sleep(4)  # let any residual at-least-once redelivery settle
            count_a = self._record_count(kafka_bootstrap_server)
            time.sleep(4)  # a few more 1s poll cycles
            count_b = self._record_count(kafka_bootstrap_server)
            assert count_b == count_a, (
                f"Record count grew from {count_a} to {count_b} across "
                "additional poll cycles — the idempotent repository is "
                "evicting and reprocessing already-seen files."
            )
        finally:
            try:
                client.delete_connector(connector_name)
            except Exception:
                pass
