# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Image-level plugin discovery tests for artemis/cp-kafka-connect:latest.

Verifies that all connector plugins and SMTs required by the system are
present in the image, regardless of which subsystem uses them.

Prerequisites:
    bazel run //tools/oci/images:kafka-connect.tarball
"""

from __future__ import annotations

import pytest
import httpx


def _list_plugin_classes(kafka_connect_url: str) -> set[str]:
    resp = httpx.get(
        f"{kafka_connect_url}/connector-plugins",
        params={"connectorsOnly": "false"},
        timeout=10,
    )
    resp.raise_for_status()
    return {p["class"] for p in resp.json()}


@pytest.mark.integration
@pytest.mark.parametrize(
    "plugin_class",
    [
        # ── Connector plugins ────────────────────────────────────────────────
        pytest.param(
            "io.aiven.kafka.connect.http.HttpSinkConnector",
            id="AivenHttpSink",
        ),
        pytest.param(
            "io.debezium.connector.postgresql.PostgresConnector",
            id="DebeziumPostgresSource",
        ),
        pytest.param(
            "io.debezium.connector.jdbc.JdbcSinkConnector",
            id="DebeziumJdbcSink",
        ),
        pytest.param(
            "org.apache.camel.kafkaconnector.springrabbitmqsink.CamelSpringrabbitmqsinkSinkConnector",  # noqa: E501
            id="CamelRabbitMQSink",
        ),
        pytest.param(
            "org.apache.camel.kafkaconnector.file.CamelFileSourceConnector",
            id="CamelFileSource",
        ),
        pytest.param(
            "org.apache.camel.kafkaconnector.filewatchsource.CamelFilewatchsourceSourceConnector",  # noqa: E501
            id="CamelFileWatchSource",
        ),
        # ── SMTs ─────────────────────────────────────────────────────────────
        pytest.param(
            "io.debezium.transforms.HeaderToValue",
            id="DebeziumHeaderToValue",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.InsertHeader",
            id="InsertHeader",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.DropHeaders",
            id="DropHeaders",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.HeaderFrom$Key",
            id="HeaderFromKey",
        ),
        pytest.param(
            "org.apache.kafka.connect.transforms.HeaderFrom$Value",
            id="HeaderFromValue",
        ),
        pytest.param(
            "io.debezium.transforms.ExtractNewRecordState",
            id="DebeziumExtractNewRecordState",
        ),
    ],
)
def test_plugin_present(kafka_connect_url: str, plugin_class: str) -> None:
    """Every required connector plugin and SMT must be discoverable in the image."""
    classes = _list_plugin_classes(kafka_connect_url)
    assert (
        plugin_class in classes
    ), f"{plugin_class} not found. Installed: {sorted(classes)}"
