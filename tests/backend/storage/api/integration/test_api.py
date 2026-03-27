"""Tier 2 HTTP integration tests for the storage service.

Tests the full request → router → service → Postgres/MinIO path using
TestClient against real infrastructure containers.  No service mocks.
"""

from __future__ import annotations

import io  # noqa: F401
import uuid
from datetime import datetime, timezone

import pytest  # noqa: F401
from fastapi.testclient import TestClient
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.backend.storage.api.models import IngestedFile, IngestionTaskType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _file_bytes(content: bytes = b"hello world") -> dict:
    return {"file": ("report.pdf", content, "application/pdf")}


def _seed_ingested_file(
    session_factory: async_sessionmaker[AsyncSession],
    namespace_id: uuid.UUID,
) -> uuid.UUID:
    """Insert a minimal IngestedFile row and return its id."""
    import asyncio

    file_id = uuid.uuid4()

    async def _insert():
        async with session_factory() as session:
            row = IngestedFile(
                id=file_id,
                namespace_id=namespace_id,
                task_id=uuid.uuid4(),
                filename="seed.pdf",
                path=f"{namespace_id}/{file_id}",
                content_type="application/pdf",
                size_bytes=0,
                status="SUCCESS",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(row)
            await session.commit()

    asyncio.get_event_loop().run_until_complete(_insert())
    return file_id


# ---------------------------------------------------------------------------
# POST /namespaces
# ---------------------------------------------------------------------------


class TestCreateNamespaceIntegration:
    def test_private_namespace_persisted(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 201
        body = response.json()
        ns_id = body["id"]

        # Verify round-trip: GET returns the same record
        get_resp = client.get(f"/namespaces/{ns_id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["id"] == ns_id
        assert get_resp.json()["type"] == "private"

    def test_shared_namespace_uses_deterministic_uuid5(
        self, client: TestClient, owner_id: str
    ) -> None:
        payload = {"type": "shared", "name": "my-project"}
        r1 = client.post("/namespaces", json=payload, headers={"X-Owner-Id": owner_id})
        r2 = client.post("/namespaces", json=payload, headers={"X-Owner-Id": owner_id})
        assert r1.status_code == 201
        assert r2.status_code == 201
        # Same name → same UUID5 seed → same ID
        assert r1.json()["id"] == r2.json()["id"]

    def test_missing_owner_id_header_returns_422(self, client: TestClient) -> None:
        response = client.post("/namespaces", json={"type": "private"})
        assert response.status_code == 422

    def test_shared_without_name_returns_422(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.post(
            "/namespaces",
            json={"type": "shared"},
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 422

    def test_invalid_owner_id_returns_400(self, client: TestClient) -> None:
        response = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": "not-a-uuid"},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# GET /namespaces and GET /namespaces/{id}
# ---------------------------------------------------------------------------


class TestGetNamespaceIntegration:
    def test_get_missing_namespace_returns_404(self, client: TestClient) -> None:
        response = client.get(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 404

    def test_list_returns_only_owner_namespaces(self, client: TestClient) -> None:
        owner_a = str(uuid.uuid4())
        owner_b = str(uuid.uuid4())

        client.post(
            "/namespaces", json={"type": "private"}, headers={"X-Owner-Id": owner_a}
        )
        client.post(
            "/namespaces", json={"type": "private"}, headers={"X-Owner-Id": owner_a}
        )
        client.post(
            "/namespaces", json={"type": "private"}, headers={"X-Owner-Id": owner_b}
        )

        resp_a = client.get("/namespaces", headers={"X-Owner-Id": owner_a})
        resp_b = client.get("/namespaces", headers={"X-Owner-Id": owner_b})

        assert resp_a.status_code == 200
        assert len(resp_a.json()) == 2
        assert resp_b.status_code == 200
        assert len(resp_b.json()) == 1

    def test_list_missing_owner_id_header_returns_422(self, client: TestClient) -> None:
        response = client.get("/namespaces")
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# PATCH /namespaces/{id}
# ---------------------------------------------------------------------------


class TestRenameNamespaceIntegration:
    def test_rename_private_updates_name_in_db(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        patch_resp = client.patch(f"/namespaces/{ns_id}", json={"name": "renamed"})
        assert patch_resp.status_code == 200

        get_resp = client.get(f"/namespaces/{ns_id}")
        assert get_resp.json()["name"] == "renamed"

    def test_rename_shared_returns_409(self, client: TestClient, owner_id: str) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "shared", "name": "shared-ns"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.patch(f"/namespaces/{ns_id}", json={"name": "new-name"})
        assert response.status_code == 409

    def test_rename_missing_namespace_returns_404(self, client: TestClient) -> None:
        response = client.patch(f"/namespaces/{uuid.uuid4()}", json={"name": "x"})
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /namespaces/{id}
# ---------------------------------------------------------------------------


class TestDeleteNamespaceIntegration:
    def test_soft_delete_makes_namespace_unreachable(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        del_resp = client.delete(f"/namespaces/{ns_id}")
        assert del_resp.status_code == 202

        assert client.get(f"/namespaces/{ns_id}").status_code == 404

    def test_deleted_namespace_excluded_from_list(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]
        client.delete(f"/namespaces/{ns_id}")

        resp = client.get("/namespaces", headers={"X-Owner-Id": owner_id})
        assert resp.status_code == 200
        assert all(ns["id"] != ns_id for ns in resp.json())

    def test_delete_missing_namespace_returns_404(self, client: TestClient) -> None:
        response = client.delete(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 404

    def test_double_delete_returns_404(self, client: TestClient, owner_id: str) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]
        client.delete(f"/namespaces/{ns_id}")
        response = client.delete(f"/namespaces/{ns_id}")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# POST /namespaces/{id}/files  (upload)
# ---------------------------------------------------------------------------


class TestUploadFileIntegration:
    def test_upload_creates_s3_object_with_create_metadata(
        self,
        client: TestClient,
        owner_id: str,
        test_minio_client: Minio,
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.post(
            f"/namespaces/{ns_id}/files",
            files=_file_bytes(),
        )
        assert response.status_code == 202
        s3_key = response.json()["s3_key"]
        task_id = response.json()["task_id"]

        import os

        bucket = os.environ["S3_ARTEMIS_BUCKET"]
        stat = test_minio_client.stat_object(bucket, s3_key)
        assert stat.metadata.get("x-amz-meta-task_id") == task_id
        assert stat.metadata.get("x-amz-meta-task_type") == IngestionTaskType.CREATE

    def test_upload_s3_key_uses_namespace_prefix(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.post(f"/namespaces/{ns_id}/files", files=_file_bytes())
        assert response.status_code == 202
        s3_key = response.json()["s3_key"]

        prefix, file_part = s3_key.split("/", 1)
        assert prefix == ns_id
        uuid.UUID(file_part)  # raises ValueError if not a valid UUID

    def test_upload_to_missing_namespace_returns_404(self, client: TestClient) -> None:
        response = client.post(
            f"/namespaces/{uuid.uuid4()}/files",
            files=_file_bytes(),
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# PUT /namespaces/{id}/files/{file_id}  (reingest)
# ---------------------------------------------------------------------------


class TestReingestFileIntegration:
    def test_reingest_overwrites_s3_object_with_modify_metadata(
        self,
        client: TestClient,
        owner_id: str,
        test_minio_client: Minio,
        storage_session_factory,
    ) -> None:
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        file_id = _seed_ingested_file(storage_session_factory, ns_id)

        response = client.put(
            f"/namespaces/{ns_id}/files/{file_id}",
            files=_file_bytes(b"updated content"),
        )
        assert response.status_code == 202
        s3_key = response.json()["s3_key"]

        import os

        stat = test_minio_client.stat_object(os.environ["S3_ARTEMIS_BUCKET"], s3_key)
        assert stat.metadata.get("x-amz-meta-task_type") == IngestionTaskType.MODIFY

    def test_reingest_missing_file_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.put(
            f"/namespaces/{ns_id}/files/{uuid.uuid4()}",
            files=_file_bytes(),
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /namespaces/{id}/files/{file_id}  (tombstone)
# ---------------------------------------------------------------------------


class TestDeleteFileIntegration:
    def test_delete_creates_zero_byte_tombstone_with_delete_metadata(
        self,
        client: TestClient,
        owner_id: str,
        test_minio_client: Minio,
        storage_session_factory,
    ) -> None:
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        file_id = _seed_ingested_file(storage_session_factory, ns_id)
        s3_key = f"{ns_id}/{file_id}"

        response = client.delete(f"/namespaces/{ns_id}/files/{file_id}")
        assert response.status_code == 202

        import os

        stat = test_minio_client.stat_object(os.environ["S3_ARTEMIS_BUCKET"], s3_key)
        assert stat.size == 0
        assert stat.metadata.get("x-amz-meta-task_type") == IngestionTaskType.DELETE

    def test_delete_missing_file_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.delete(f"/namespaces/{ns_id}/files/{uuid.uuid4()}")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /namespaces/{id}/files
# ---------------------------------------------------------------------------


class TestListFilesIntegration:
    def test_list_returns_seeded_files(
        self,
        client: TestClient,
        owner_id: str,
        storage_session_factory,
    ) -> None:
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        _seed_ingested_file(storage_session_factory, ns_id)
        _seed_ingested_file(storage_session_factory, ns_id)

        response = client.get(f"/namespaces/{ns_id}/files")
        assert response.status_code == 200
        assert len(response.json()) == 2

    def test_list_excludes_other_namespace_files(
        self,
        client: TestClient,
        owner_id: str,
        storage_session_factory,
    ) -> None:
        ns_a = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        ns_b = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        _seed_ingested_file(storage_session_factory, ns_a)
        _seed_ingested_file(storage_session_factory, ns_b)

        resp_a = client.get(f"/namespaces/{ns_a}/files")
        assert len(resp_a.json()) == 1
        assert resp_a.json()[0]["namespace_id"] == str(ns_a)

    def test_list_files_returns_correct_fields(
        self,
        client: TestClient,
        owner_id: str,
        storage_session_factory,
    ) -> None:
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        file_id = _seed_ingested_file(storage_session_factory, ns_id)

        response = client.get(f"/namespaces/{ns_id}/files")
        assert response.status_code == 200
        files = response.json()
        assert len(files) == 1
        f = files[0]
        assert f["id"] == str(file_id)
        assert f["namespace_id"] == str(ns_id)
        assert f["filename"] == "seed.pdf"
        assert f["content_type"] == "application/pdf"
        assert f["size_bytes"] == 0
        assert f["status"] == "SUCCESS"
        assert f["failure_reason"] is None
        assert "task_id" in f
        assert "completed_at" in f

    def test_list_files_on_missing_namespace_returns_404(
        self, client: TestClient
    ) -> None:
        response = client.get(f"/namespaces/{uuid.uuid4()}/files")
        assert response.status_code == 404
