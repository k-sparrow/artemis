"""KafkaConnect container tests for the artemis/cp-kafka-connect image.

Verifies that the custom-built KafkaConnect image:
  1. Ships the CamelFileSource and CamelFileWatchSource connector plugins.
  2. Can deploy a FileSource connector using the production template and reach RUNNING.
  3. Produces messages with the correct headers (artemis.namespace, artemis.org_name,
     CamelHeader.CamelFileAbsolutePath).

Pre-requisite:
    artemis/cp-kafka-connect:latest must be built.
"""

from __future__ import annotations

import time
import uuid
from typing import Iterator

import pytest
from kafka import KafkaConsumer, TopicPartition
from kafka_connect import KafkaConnect

from src.backend.enterprise.data_sources.api.sources.templates import (
    render_filesystem_connector,
)

_TOPIC = "artemis.datasource.filesystem"
_NAMESPACE = "test-ns"
_ORG_NAME = "test-org"


def _kc_client(kafka_connect_url: str) -> KafkaConnect:
    return KafkaConnect(url=kafka_connect_url)


def _wait_connector_running(
    client: KafkaConnect, name: str, timeout: int = 60
) -> None:
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


@pytest.mark.parametrize("connector_class", [
    pytest.param(
        "org.apache.camel.kafkaconnector.file.CamelFileSourceConnector",
        id="CamelFileSource",
    ),
    pytest.param(
        "org.apache.camel.kafkaconnector.filewatchsource.CamelFilewatchsourceSourceConnector",
        id="CamelFileWatchSource",
    ),
])
def test_connector_plugin_installed(kafka_connect_url: str, connector_class: str) -> None:
    """artemis/cp-kafka-connect must ship the required Camel connector plugins."""
    plugins = _kc_client(kafka_connect_url).list_connector_plugins()
    classes = {p["class"] for p in plugins}
    assert connector_class in classes, (
        f"{connector_class} not found. Installed: {sorted(classes)}"
    )


class TestFilesourceConnector:
    """Deploy our production FileSource template and verify runtime behaviour."""

    @pytest.fixture(autouse=True)
    def connector(self, kafka_connect_url: str) -> Iterator[KafkaConnect]:
        """Deploy a uniquely-named connector per test to avoid stale offsets."""
        client = _kc_client(kafka_connect_url)
        connector_name = f"test-filesource-{uuid.uuid4().hex[:8]}"
        config = render_filesystem_connector(
            connector_name=connector_name,
            watch_path="/watch",
            namespace=_NAMESPACE,
            org_name=_ORG_NAME,
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

    def test_connector_reaches_running_state(
        self, connector: KafkaConnect
    ) -> None:
        _wait_connector_running(connector, self._connector_name)

    @pytest.mark.parametrize("header,expected_value", [
        pytest.param("artemis.namespace", _NAMESPACE, id="namespace"),
        pytest.param("artemis.org_name", _ORG_NAME, id="org_name"),
    ])
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
            assert header in headers, (
                f"{header!r} header missing. headers={headers}"
            )
            assert headers[header].decode() == expected_value

    def test_file_path_header_present_and_correct(
        self, connector: KafkaConnect, kafka_bootstrap_server: str
    ) -> None:
        _wait_connector_running(connector, self._connector_name)
        msgs = self._consume_messages(kafka_bootstrap_server)
        assert len(msgs) >= 1
        for msg in msgs:
            headers = dict(msg.headers)
            assert "CamelHeader.CamelFileAbsolutePath" in headers, (
                f"CamelHeader.CamelFileAbsolutePath missing. headers={headers}"
            )
            path = headers["CamelHeader.CamelFileAbsolutePath"].decode()
            assert path.startswith("/watch/"), (
                f"Expected path under /watch/, got: {path}"
            )
