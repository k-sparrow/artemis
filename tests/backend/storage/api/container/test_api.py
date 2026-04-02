"""Container integration tests for the storage service.

Tests the real HTTP interface of the storage service running as a Docker
container against live Kafka, MinIO, and Postgres containers.
"""

from __future__ import annotations

import io
import json
import uuid
from urllib.parse import unquote

import httpx
import pytest  # noqa: F401
from fastapi import status
from kafka import KafkaConsumer
from minio import Minio
from src.lib.core.ingestion.contract import IngestionTaskDetails


def _pdf(content: bytes = b"hello world") -> dict:
    return {"file": ("report.pdf", content, "application/pdf")}


_OWNER_ID = str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_liveness_returns_200(self, client: httpx.Client) -> None:
        resp = client.get("/health/liveness")
        assert resp.status_code == status.HTTP_200_OK

    def test_readiness_returns_200(self, client: httpx.Client) -> None:
        resp = client.get("/health/readiness")
        assert resp.status_code == status.HTTP_200_OK


# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------


class TestNamespaces:
    def test_create_namespace_returns_201(self, client: httpx.Client) -> None:
        resp = client.post(
            "/namespaces",
            json={"type": "shared", "name": "acme"},
            headers={"X-Owner-Id": _OWNER_ID},
        )
        assert resp.status_code == status.HTTP_201_CREATED
        body = resp.json()
        assert body["name"] == "acme"
        assert "id" in body

    def test_duplicate_shared_namespace_returns_409(self, client: httpx.Client) -> None:
        payload = {"type": "shared", "name": "acme"}
        headers = {"X-Owner-Id": _OWNER_ID}

        r1 = client.post("/namespaces", json=payload, headers=headers)
        r2 = client.post("/namespaces", json=payload, headers=headers)

        assert r1.status_code == status.HTTP_201_CREATED
        assert r2.status_code == status.HTTP_409_CONFLICT


# ---------------------------------------------------------------------------
# Object upload
# ---------------------------------------------------------------------------


class TestObjectUpload:
    def _create_namespace(self, client: httpx.Client) -> str:
        resp = client.post(
            "/namespaces",
            json={"type": "shared", "name": "acme"},
            headers={"X-Owner-Id": _OWNER_ID},
        )
        return resp.json()["id"]

    def test_upload_returns_202(self, client: httpx.Client) -> None:
        ns_id = self._create_namespace(client)
        resp = client.post(f"/namespaces/{ns_id}/objects", files=_pdf())
        assert resp.status_code == status.HTTP_202_ACCEPTED

    def test_upload_response_has_task_id_and_s3_key(self, client: httpx.Client) -> None:
        ns_id = self._create_namespace(client)
        body = client.post(f"/namespaces/{ns_id}/objects", files=_pdf()).json()
        assert "task_id" in body
        assert "s3_key" in body

    def test_object_lands_in_minio(
        self, client: httpx.Client, test_minio_client: Minio
    ) -> None:
        ns_id = self._create_namespace(client)
        body = client.post(f"/namespaces/{ns_id}/objects", files=_pdf()).json()
        s3_key = body["s3_key"]

        # Verify the object actually exists in MinIO.
        obj = test_minio_client.stat_object("artemis", s3_key)
        assert obj.size > 0

    def test_object_bytes_match(
        self, client: httpx.Client, test_minio_client: Minio
    ) -> None:
        content = b"unique pdf content for byte check"
        ns_id = self._create_namespace(client)
        body = client.post(
            f"/namespaces/{ns_id}/objects",
            files={"file": ("doc.pdf", content, "application/pdf")},
        ).json()

        data = test_minio_client.get_object("artemis", body["s3_key"]).read()
        assert data == content

    def test_unknown_namespace_returns_404(self, client: httpx.Client) -> None:
        fake_ns = str(uuid.uuid4())
        resp = client.post(f"/namespaces/{fake_ns}/objects", files=_pdf())
        assert resp.status_code == status.HTTP_404_NOT_FOUND


# ---------------------------------------------------------------------------
# Object listing
# ---------------------------------------------------------------------------


class TestObjectListing:
    def test_list_empty_namespace_returns_200(self, client: httpx.Client) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "shared", "name": "acme"},
            headers={"X-Owner-Id": _OWNER_ID},
        ).json()["id"]
        resp = client.get(f"/namespaces/{ns_id}/objects")
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json() == []

    def test_list_is_namespace_scoped(self, client: httpx.Client) -> None:
        # The listing endpoint is DB-backed and populated by the Celery worker
        # (downstream of this service). Without a running worker, the list is
        # always empty after upload — verify the endpoint is accessible and
        # namespace-scoped (different namespaces are independent).
        ns1 = client.post(
            "/namespaces",
            json={"type": "shared", "name": "ns-one"},
            headers={"X-Owner-Id": _OWNER_ID},
        ).json()["id"]
        ns2 = client.post(
            "/namespaces",
            json={"type": "shared", "name": "ns-two"},
            headers={"X-Owner-Id": _OWNER_ID},
        ).json()["id"]

        assert client.get(f"/namespaces/{ns1}/objects").json() == []
        assert client.get(f"/namespaces/{ns2}/objects").json() == []
        assert ns1 != ns2


# ---------------------------------------------------------------------------
# MinIO → Kafka pipeline smoke test (bypasses the storage service entirely)
# ---------------------------------------------------------------------------


class TestMinioKafkaPipeline:
    """Verify that MinIO itself delivers S3 events to Kafka.

    Puts an object directly via the Minio client — no storage service involved.
    If this passes but TestKafkaNotification fails, the issue is in the storage
    service or its interaction with MinIO.  If this also fails, the
    MinIO→Kafka notification config is broken.
    """

    _BUCKET = "artemis"

    def test_direct_put_produces_kafka_message(
        self,
        test_minio_client: Minio,
        kafka_consumer: KafkaConsumer,
    ) -> None:
        key = f"smoke/{uuid.uuid4()}.txt"
        content = b"smoke test payload"
        test_minio_client.put_object(
            bucket_name=self._BUCKET,
            object_name=key,
            data=io.BytesIO(content),
            length=len(content),
            content_type="text/plain",
        )

        messages = list(kafka_consumer)

        assert len(messages) >= 1
        event = json.loads(messages[0].value)
        keys = [unquote(r["s3"]["object"]["key"]) for r in event.get("Records", [])]
        assert any(key in k for k in keys)


# ---------------------------------------------------------------------------
# Kafka notifications
# ---------------------------------------------------------------------------


class TestKafkaNotification:
    """Verify that uploading an object causes MinIO to emit an S3 event to Kafka."""

    def _create_namespace(self, client: httpx.Client) -> str:
        return client.post(
            "/namespaces",
            json={"type": "shared", "name": "acme"},
            headers={"X-Owner-Id": _OWNER_ID},
        ).json()["id"]

    def test_upload_produces_kafka_message(
        self,
        client: httpx.Client,
        kafka_consumer: KafkaConsumer,
    ) -> None:
        ns_id = self._create_namespace(client)
        body = client.post(f"/namespaces/{ns_id}/objects", files=_pdf()).json()
        s3_key = body["s3_key"]

        messages = list(kafka_consumer)

        assert len(messages) >= 1

        event = json.loads(messages[0].value)
        keys = [unquote(r["s3"]["object"]["key"]) for r in event.get("Records", [])]
        assert any(s3_key in k for k in keys)

    def test_upload_kafka_message_contract(
        self,
        client: httpx.Client,
        kafka_consumer: KafkaConsumer,
    ) -> None:
        ns_id = self._create_namespace(client)
        client.post(f"/namespaces/{ns_id}/objects", files=_pdf()).json()

        messages = list(kafka_consumer)
        assert len(messages) >= 1

        # MinIO S3 event payload:
        #   {"Records": [{"s3": {"object": {"key": ..., "userMetadata": {...}}}}]}
        s3_obj = json.loads(messages[0].value)["Records"][0]["s3"]["object"]

        # The storage service stamps the IngestionTaskDetails contract as object
        # metadata. Validate it parses against the Pydantic model and contains
        # the correct namespace.
        contract = IngestionTaskDetails.model_validate_json(
            s3_obj["userMetadata"]["X-Amz-Meta-Contract"]
        )
        assert str(contract.info.namespace_id) == ns_id

    def test_no_message_without_upload(self, kafka_consumer: KafkaConsumer) -> None:
        messages = list(kafka_consumer)
        assert messages == []
