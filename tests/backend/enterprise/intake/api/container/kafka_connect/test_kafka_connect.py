"""KafkaConnect container tests for the HTTP sink connector.

Verifies that the artemis/cp-kafka-connect image:
  1. Ships the Aiven HTTP sink and Debezium HeaderToValue plugins.
  2. Can deploy our HTTP sink connector template and reach RUNNING.
  3. Delivers a message from ``artemis.datasource.filesystem.intake``
     (the ksqlDB-shaped topic) to the intake endpoint verbatim:
       - JSON body contains source, display_name, namespace_id
       - Body matches the IntakeRequest schema expected by POST /intake

Pre-requisite:
    artemis/cp-kafka-connect:latest must be built.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Iterator

import httpx
import pytest
from kafka import KafkaProducer
from kafka_connect import KafkaConnect

_INTAKE_TOPIC = "artemis.datasource.filesystem.intake"


def _render_http_sink_connector(*, connector_name: str, intake_url: str) -> dict:
    return {
        "name": connector_name,
        "config": {
            "connector.class": "io.aiven.kafka.connect.http.HttpSinkConnector",
            "tasks.max": "1",
            "topics": _INTAKE_TOPIC,
            "http.url": intake_url,
            "http.authorization.type": "none",
            "batching.enabled": "false",
            "consumer.override.auto.offset.reset": "latest",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.tolerance": "all",
            "errors.deadletterqueue.topic.name": "artemis.datasource.filesystem.intake.dlq",  # noqa: E501
            "errors.deadletterqueue.topic.replication.factor": "1",
            "errors.deadletterqueue.context.headers.enable": "true",
        },
    }


_NAMESPACE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_FILE_PATH = "/watch/doc1.txt"
_FILE_NAME = "doc1.txt"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _kc_client(kafka_connect_url: str) -> KafkaConnect:
    return KafkaConnect(url=kafka_connect_url)


def _list_all_plugins(kafka_connect_url: str) -> set[str]:
    """Return all plugin classes including SMTs and converters."""
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


def _wait_for_wiremock_post(
    wiremock_host_url: str,
    path: str = "/intake",
    timeout: int = 30,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
        calls = [
            r
            for r in resp.json().get("requests", [])
            if r["request"]["method"] == "POST" and r["request"]["url"] == path
        ]
        if calls:
            return calls[0]["request"]
        time.sleep(1)
    pytest.fail(
        f"WireMock received no POST {path} within {timeout}s. "
        f"Recorded: {httpx.get(f'{wiremock_host_url}/__admin/requests').json()}"
    )


# ---------------------------------------------------------------------------
# Plugin presence
# ---------------------------------------------------------------------------


class TestArtemisKafkaConnectBundle:
    @pytest.mark.parametrize(
        "plugin_class",
        [
            pytest.param(
                "io.aiven.kafka.connect.http.HttpSinkConnector",
                id="AivenHttpSink",
            ),
            pytest.param(
                "io.debezium.transforms.HeaderToValue",
                id="DebeziumHeaderToValue",
            ),
        ],
    )
    def test_required_plugin_installed(
        self, kafka_connect_url: str, plugin_class: str
    ) -> None:
        classes = _list_all_plugins(kafka_connect_url)
        assert (
            plugin_class in classes
        ), f"{plugin_class} not found. Installed: {sorted(classes)}"


# ---------------------------------------------------------------------------
# HTTP sink connector — deployment and JSON body delivery
# ---------------------------------------------------------------------------


class TestHttpSinkConnector:
    @pytest.fixture(autouse=True)
    def connector(
        self,
        kafka_connect_url: str,
        wiremock_internal_url: str,
        kafka_topic: str,
    ) -> Iterator[KafkaConnect]:
        client = _kc_client(kafka_connect_url)
        connector_name = f"test-http-sink-{uuid.uuid4().hex[:8]}"
        config = _render_http_sink_connector(
            connector_name=connector_name,
            intake_url=f"{wiremock_internal_url}/intake",
        )
        client.create_connector(config)
        self._connector_name = connector_name
        self._kc_client = client
        yield client
        try:
            client.delete_connector(connector_name)
        except Exception:
            pass

    def test_connector_reaches_running(self) -> None:
        _wait_connector_running(self._kc_client, self._connector_name)

    def test_json_body_delivered_verbatim(
        self,
        kafka_bootstrap_server: str,
        wiremock_host_url: str,
        kafka_topic: str,
    ) -> None:
        """
        Produce an IntakeRequest-shaped JSON record;
        assert WireMock receives it intact.
        """
        _wait_connector_running(self._kc_client, self._connector_name)

        payload = {
            "source": {"type": "filesystem", "path": _FILE_PATH},
            "display_name": _FILE_NAME,
            "namespace_id": _NAMESPACE_ID,
        }

        producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(_INTAKE_TOPIC, value=payload)
        producer.flush()
        producer.close()

        request = _wait_for_wiremock_post(wiremock_host_url)
        body = json.loads(request["body"])

        assert body["source"] == {"type": "filesystem", "path": _FILE_PATH}
        assert body["display_name"] == _FILE_NAME
        assert body["namespace_id"] == _NAMESPACE_ID

    def test_multiple_messages_each_delivered(
        self,
        kafka_bootstrap_server: str,
        wiremock_host_url: str,
        kafka_topic: str,
    ) -> None:
        """Two records on the intake topic → two POSTs to the intake endpoint."""
        _wait_connector_running(self._kc_client, self._connector_name)

        producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        filenames = ["file_a.txt", "file_b.txt"]
        for filename in filenames:
            producer.send(
                _INTAKE_TOPIC,
                value={
                    "source": {"type": "filesystem", "path": f"/watch/{filename}"},
                    "display_name": filename,
                    "namespace_id": _NAMESPACE_ID,
                },
            )
        producer.flush()
        producer.close()

        _wait_for_wiremock_post(wiremock_host_url)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
            calls = [
                r
                for r in resp.json().get("requests", [])
                if r["request"]["method"] == "POST" and r["request"]["url"] == "/intake"
            ]
            if len(calls) >= 2:
                break
            time.sleep(1)
        else:
            pytest.fail("Expected 2 POST /intake calls, WireMock received fewer")

        display_names = {
            json.loads(c["request"]["body"])["display_name"] for c in calls
        }
        assert display_names == set(filenames)
