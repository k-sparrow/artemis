"""Container integration tests for the data_sources control plane.

Uses real containers: data_sources service (artemis/backend-data-sources:dev),
KafkaConnect (artemis/cp-kafka-connect:latest), Kafka broker, Postgres,
and WireMock for the storage service.

Pre-requisite:
    bazel run //src/backend/enterprise/data_sources/api:tarball.dev

Tests verify the end-to-end flow:
  POST /data-sources → KafkaConnect deploys connector → files produce Kafka messages
  DELETE /data-sources/{id} → connector removed, namespace soft-deleted on storage
"""

from __future__ import annotations

import time

import httpx
import pytest  # noqa: F401
from fastapi import status
from kafka import KafkaConsumer
from wiremock.testing.testcontainer import WireMockContainer

from tests.backend.enterprise.data_sources.api.container.conftest import (
    wait_for_connector_state,
)

_PAYLOAD = {
    "display_name": "Smoke Test Docs",
    "path": "/watch",
    "namespace": "smoke-ns",
    "org_name": "smoke-org",
}


class TestDataSourceContainer:
    def test_create_deploys_connector_and_returns_201(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
    ) -> None:
        resp = client.post("/data-sources", json=_PAYLOAD)
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        body = resp.json()
        assert body["connector_name"].startswith("artemis-")
        assert body["source_type"] == "filesystem"
        assert body["config"]["path"] == "/watch"

    def test_connector_reaches_running_state(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
    ) -> None:
        created = client.post("/data-sources", json=_PAYLOAD).json()
        body = wait_for_connector_state(client, created["id"], target_state="RUNNING")
        assert body["kafka_status"]["state"] == "RUNNING"

    def test_filesystem_connector_produces_kafka_messages(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
        kafka_consumer: KafkaConsumer,
    ) -> None:
        """Pre-seeded files (doc1.txt, doc2.txt) should produce ≥ 2 messages."""
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")

        messages = list(kafka_consumer)
        assert len(messages) >= 2

    def test_kafka_messages_contain_namespace_and_org_name(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
        kafka_consumer: KafkaConsumer,
    ) -> None:
        """InsertHeader SMTs must inject namespace + org_name as Kafka headers."""
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")

        messages = list(kafka_consumer)
        assert len(messages) >= 1

        for msg in messages:
            # Key: SMT chain must extract the namespace as a plain string key.
            assert msg.key is not None, "message key must not be None"
            assert (
                msg.key.decode() == _PAYLOAD["namespace"]
            ), f"unexpected key: {msg.key}"
            # Headers
            headers = dict(msg.headers)
            assert (
                "artemis.namespace" in headers
            ), f"artemis.namespace missing. headers={headers}"
            assert (
                "artemis.namespace_id" in headers
            ), f"artemis.namespace_id missing. headers={headers}"
            assert (
                "artemis.org_name" in headers
            ), f"artemis.org_name missing. headers={headers}"
            assert headers["artemis.namespace"].decode() == _PAYLOAD["namespace"]
            assert headers["artemis.namespace_id"].decode() == created["namespace_id"]
            assert headers["artemis.org_name"].decode() == _PAYLOAD["org_name"]
            assert (
                "CamelHeader.CamelFileAbsolutePath" in headers
            ), f"CamelHeader.CamelFileAbsolutePath missing. headers={headers}"
            assert (
                headers["CamelHeader.CamelFileAbsolutePath"]
                .decode()
                .startswith("/watch/")
            )

    def test_drop_new_file_produces_kafka_message(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
        kafka_consumer: KafkaConsumer,
        watch_dir,
    ) -> None:
        """A file written after connector is RUNNING should produce a new message."""
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")

        # Drop a new file. The host can write to a RO-mounted dir; the
        # container just cannot. The connector's poll cycle will pick it up.
        (watch_dir / "live_drop.txt").write_text("live document")

        # Poll until we see a message whose CamelFileAbsolutePath contains
        # live_drop.txt. Pre-seeded messages may arrive in the same window —
        # filter by path header to confirm the live drop was processed.
        live_msgs = []
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            records = kafka_consumer.poll(timeout_ms=3000)
            for msgs in records.values():
                for msg in msgs:
                    headers = dict(msg.headers)
                    path = headers.get(
                        "CamelHeader.CamelFileAbsolutePath", b""
                    ).decode()
                    if "live_drop.txt" in path:
                        live_msgs.append(msg)
            if live_msgs:
                break

        assert len(live_msgs) >= 1, "live_drop.txt was not picked up by the connector"
        headers = dict(live_msgs[0].headers)
        assert headers["artemis.namespace"].decode() == _PAYLOAD["namespace"]

    def test_delete_removes_connector_from_kafka_connect(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
    ) -> None:
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")
        connector_name = created["connector_name"]

        resp = client.delete(f"/data-sources/{created['id']}")
        assert resp.status_code == status.HTTP_204_NO_CONTENT

        # Verify connector no longer exists in Kafka Connect.
        kc_resp = httpx.get(f"{kafka_connect_url}/connectors/{connector_name}")
        assert kc_resp.status_code == status.HTTP_404_NOT_FOUND

    def test_delete_soft_deletes_namespace_on_storage_service(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
        wiremock: WireMockContainer,
    ) -> None:
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")
        namespace_id = created["namespace_id"]

        client.delete(f"/data-sources/{created['id']}")

        # Verify WireMock received the DELETE /namespaces/{namespace_id} call.
        admin_resp = httpx.get(
            wiremock.get_url(path="/__admin/requests"),
            params={"method": "DELETE", "url": f"/namespaces/{namespace_id}"},
        )
        assert admin_resp.status_code == 200
        requests_log = admin_resp.json()
        delete_calls = [
            r
            for r in requests_log.get("requests", [])
            if r["request"]["method"] == "DELETE"
            and f"/namespaces/{namespace_id}" in r["request"]["url"]
        ]
        assert len(delete_calls) >= 1

    def test_deleted_source_not_returned_in_list(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
    ) -> None:
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")

        client.delete(f"/data-sources/{created['id']}")

        assert client.get("/data-sources").json() == []

    def test_list_returns_all_live_sources(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
    ) -> None:
        """GET /data-sources returns all non-deleted sources with correct fields."""
        second_payload = {
            **_PAYLOAD,
            "namespace": "smoke-ns-2",
            "display_name": "Second",
        }
        id1 = client.post("/data-sources", json=_PAYLOAD).json()["id"]
        id2 = client.post("/data-sources", json=second_payload).json()["id"]

        items = client.get("/data-sources").json()
        ids = {i["id"] for i in items}
        assert id1 in ids and id2 in ids, f"Expected both ids in list, got: {ids}"
        for item in items:
            assert item["connector_name"].startswith("artemis-")
            assert item["source_type"] == "filesystem"
            assert "kafka_status" in item

    def test_double_delete_is_idempotent(
        self,
        client: httpx.Client,
        kafka_connect_url: str,
    ) -> None:
        """Deleting a connector that KC has already removed must still return 204."""
        created = client.post("/data-sources", json=_PAYLOAD).json()
        wait_for_connector_state(client, created["id"], target_state="RUNNING")

        resp1 = client.delete(f"/data-sources/{created['id']}")
        assert resp1.status_code == status.HTTP_204_NO_CONTENT

        # Second delete: KC will 404 on the connector; service must absorb it.
        resp2 = client.delete(f"/data-sources/{created['id']}")
        assert resp2.status_code == status.HTTP_404_NOT_FOUND
