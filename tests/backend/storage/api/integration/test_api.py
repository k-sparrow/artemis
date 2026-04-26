"""Tier 2 HTTP integration tests for the storage service.

Tests the full request → router → service → Postgres/MinIO path using
TestClient against real infrastructure containers.  No service mocks.
"""

from __future__ import annotations

import json
import uuid
from fastapi.testclient import TestClient
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.backend.storage.api.models import IngestedObject, IngestionTaskType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _file_bytes(content: bytes = b"hello world") -> dict:
    return {"file": ("report.pdf", content, "application/pdf")}


def _seed_ingested_object(
    session_factory: async_sessionmaker[AsyncSession],
    namespace_id: uuid.UUID,
    source: str = "seed.pdf",
    group_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Insert a minimal IngestedObject row and return its id."""
    import asyncio

    obj_id = uuid.uuid5(namespace_id, source)

    async def _insert():
        async with session_factory() as session:
            row = IngestedObject(
                id=obj_id,
                namespace_id=namespace_id,
                source=source,
                object_type="file",
                s3_key=f"{namespace_id}/{obj_id}",
                content_type="application/pdf",
                size_bytes=0,
                group_id=group_id,
            )
            session.add(row)
            await session.commit()

    asyncio.get_event_loop().run_until_complete(_insert())
    return obj_id


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
        _ARTEMIS_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        name = "my-project"
        expected_id = str(uuid.uuid5(_ARTEMIS_NS, name))

        r1 = client.post(
            "/namespaces",
            json={"type": "shared", "name": name},
            headers={"X-Owner-Id": owner_id},
        )
        assert r1.status_code == 201
        assert r1.json()["id"] == expected_id

        # Duplicate → 409; body contains the existing namespace so callers don't
        # need to re-derive the UUID5 themselves.
        r2 = client.post(
            "/namespaces",
            json={"type": "shared", "name": name},
            headers={"X-Owner-Id": owner_id},
        )
        assert r2.status_code == 409
        assert r2.json()["id"] == expected_id

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

    def test_get_by_name_returns_shared_namespace(
        self, client: TestClient, owner_id: str
    ) -> None:
        name = "lookup-by-name"
        created = client.post(
            "/namespaces",
            json={"type": "shared", "name": name},
            headers={"X-Owner-Id": owner_id},
        ).json()

        resp = client.get(f"/namespaces/by-name/{name}")
        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]
        assert resp.json()["name"] == name

    def test_get_by_name_missing_returns_404(self, client: TestClient) -> None:
        resp = client.get("/namespaces/by-name/nonexistent-namespace")
        assert resp.status_code == 404

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
# POST /namespaces/{id}/objects  (upload)
# ---------------------------------------------------------------------------


class TestUploadObjectIntegration:
    def test_upload_creates_s3_object_with_create_contract(
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
            f"/namespaces/{ns_id}/objects",
            files=_file_bytes(),
        )
        assert response.status_code == 202
        task_id = response.json()["task_id"]
        s3_key = f"{ns_id}/{uuid.uuid5(uuid.UUID(ns_id), 'report.pdf')}"

        import os

        bucket = os.environ["S3_ARTEMIS_BUCKET"]
        stat = test_minio_client.stat_object(bucket, s3_key)
        assert stat.metadata.get("x-amz-meta-task_id") == task_id
        contract = json.loads(stat.metadata.get("x-amz-meta-contract"))
        assert contract["upload_action"] == IngestionTaskType.CREATE

    def test_upload_to_missing_namespace_returns_404(self, client: TestClient) -> None:
        response = client.post(
            f"/namespaces/{uuid.uuid4()}/objects",
            files=_file_bytes(),
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# PUT /namespaces/{id}/objects/{obj_id}  (reingest)
# ---------------------------------------------------------------------------


class TestReingestObjectIntegration:
    def test_reingest_overwrites_s3_object_with_modify_contract(
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
        obj_id = _seed_ingested_object(storage_session_factory, ns_id)

        response = client.put(
            f"/namespaces/{ns_id}/objects/{obj_id}",
            files=_file_bytes(b"updated content"),
        )
        assert response.status_code == 202
        s3_key = f"{ns_id}/{obj_id}"

        import os

        stat = test_minio_client.stat_object(os.environ["S3_ARTEMIS_BUCKET"], s3_key)
        contract = json.loads(stat.metadata.get("x-amz-meta-contract"))
        assert contract["upload_action"] == IngestionTaskType.MODIFY

    def test_reingest_missing_object_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.put(
            f"/namespaces/{ns_id}/objects/{uuid.uuid4()}",
            files=_file_bytes(),
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /namespaces/{id}/objects/{obj_id}  (tombstone)
# ---------------------------------------------------------------------------


class TestDeleteObjectIntegration:
    def test_delete_creates_zero_byte_tombstone_with_delete_contract(
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
        obj_id = _seed_ingested_object(storage_session_factory, ns_id)
        s3_key = f"{ns_id}/{obj_id}"

        response = client.delete(f"/namespaces/{ns_id}/objects/{obj_id}")
        assert response.status_code == 202

        import os

        stat = test_minio_client.stat_object(os.environ["S3_ARTEMIS_BUCKET"], s3_key)
        assert stat.size == 0
        contract = json.loads(stat.metadata.get("x-amz-meta-contract"))
        assert contract["upload_action"] == IngestionTaskType.DELETE

    def test_delete_missing_object_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.delete(f"/namespaces/{ns_id}/objects/{uuid.uuid4()}")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /namespaces/{id}/objects
# ---------------------------------------------------------------------------


class TestListObjectsIntegration:
    def test_list_returns_seeded_objects(
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
        _seed_ingested_object(storage_session_factory, ns_id)

        response = client.get(f"/namespaces/{ns_id}/objects")
        assert response.status_code == 200
        assert len(response.json()) == 1

    def test_list_excludes_other_namespace_objects(
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
        _seed_ingested_object(storage_session_factory, ns_a)
        _seed_ingested_object(storage_session_factory, ns_b)

        resp_a = client.get(f"/namespaces/{ns_a}/objects")
        assert len(resp_a.json()) == 1
        assert resp_a.json()[0]["namespace_id"] == str(ns_a)

    def test_list_objects_returns_correct_fields(
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
        obj_id = _seed_ingested_object(storage_session_factory, ns_id)

        response = client.get(f"/namespaces/{ns_id}/objects")
        assert response.status_code == 200
        objects = response.json()
        assert len(objects) == 1
        o = objects[0]
        assert o["id"] == str(obj_id)
        assert o["namespace_id"] == str(ns_id)
        assert o["source"] == "seed.pdf"
        assert o["object_type"] == "file"
        assert o["content_type"] == "application/pdf"
        assert o["size_bytes"] == 0
        assert o["group_id"] is None

    def test_list_objects_on_missing_namespace_returns_404(
        self, client: TestClient
    ) -> None:
        response = client.get(f"/namespaces/{uuid.uuid4()}/objects")
        assert response.status_code == 404

    def test_list_objects_filtered_by_group_id(
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
        group_id = uuid.uuid4()
        _seed_ingested_object(
            storage_session_factory, ns_id, source="a.pdf", group_id=group_id
        )
        _seed_ingested_object(
            storage_session_factory, ns_id, source="b.pdf", group_id=None
        )

        resp = client.get(
            f"/namespaces/{ns_id}/objects", params={"group_id": str(group_id)}
        )
        assert resp.status_code == 200
        objects = resp.json()
        assert len(objects) == 1
        assert objects[0]["group_id"] == str(group_id)

    def test_upload_with_group_id_included_in_contract(
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
        group_id = uuid.uuid4()

        import os

        response = client.post(
            f"/namespaces/{ns_id}/objects",
            files=_file_bytes(),
            params={"group_id": str(group_id)},
        )
        assert response.status_code == 202
        s3_key = f"{ns_id}/{uuid.uuid5(uuid.UUID(ns_id), 'report.pdf')}"
        stat = test_minio_client.stat_object(os.environ["S3_ARTEMIS_BUCKET"], s3_key)
        contract = json.loads(stat.metadata.get("x-amz-meta-contract"))
        assert contract["info"]["group_id"] == str(group_id)
