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

from src.backend.storage.api.models import (
    IngestedObject,
    IngestionTaskType,
)
from src.lib.core.ingestion.models import IngestionStatus


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
                content_type="application/pdf",
                size_bytes=0,
                group_id=group_id,
            )
            session.add(row)
            await session.commit()

    asyncio.get_event_loop().run_until_complete(_insert())
    return obj_id


def _seed_ingestion_status(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: uuid.UUID,
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID | None = None,
    stage: str = "tasks.ingest",
    status: str = "running",
    operation: str = "CREATE",
    failure_reason: str | None = None,
) -> None:
    """Insert a minimal IngestionStatus row directly — stands in for what
    the worker's outbox (create_status_row/update_stage/mark_success/
    mark_failure) would write over the course of a real pipeline run."""
    import asyncio

    async def _insert():
        async with session_factory() as session:
            row = IngestionStatus(
                task_id=task_id,
                namespace_id=namespace_id,
                obj_id=obj_id,
                operation=operation,
                stage=stage,
                status=status,
                failure_reason=failure_reason,
            )
            session.add(row)
            await session.commit()

    asyncio.get_event_loop().run_until_complete(_insert())


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
        get_resp = client.get(f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id})
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

    def test_missing_owner_id_header_returns_401(self, client: TestClient) -> None:
        response = client.post("/namespaces", json={"type": "private"})
        assert response.status_code == 401

    def test_shared_without_name_returns_422(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.post(
            "/namespaces",
            json={"type": "shared"},
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 422

    def test_invalid_owner_id_returns_401(self, client: TestClient) -> None:
        response = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": "not-a-uuid"},
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# GET /namespaces and GET /namespaces/{id}
# ---------------------------------------------------------------------------


class TestGetNamespaceIntegration:
    def test_get_missing_namespace_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.get(
            f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": owner_id}
        )
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

        resp = client.get(
            f"/namespaces/by-name/{name}", headers={"X-Owner-Id": owner_id}
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]
        assert resp.json()["name"] == name

    def test_get_by_name_missing_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        resp = client.get(
            "/namespaces/by-name/nonexistent-namespace",
            headers={"X-Owner-Id": owner_id},
        )
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

    def test_list_missing_owner_id_header_returns_401(self, client: TestClient) -> None:
        response = client.get("/namespaces")
        assert response.status_code == 401


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

        patch_resp = client.patch(
            f"/namespaces/{ns_id}",
            json={"name": "renamed"},
            headers={"X-Owner-Id": owner_id},
        )
        assert patch_resp.status_code == 200

        get_resp = client.get(f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id})
        assert get_resp.json()["name"] == "renamed"

    def test_rename_shared_returns_409(self, client: TestClient, owner_id: str) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "shared", "name": "shared-ns"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        response = client.patch(
            f"/namespaces/{ns_id}",
            json={"name": "new-name"},
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 409

    def test_rename_missing_namespace_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.patch(
            f"/namespaces/{uuid.uuid4()}",
            json={"name": "x"},
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /namespaces/{id}
# ---------------------------------------------------------------------------


class TestDeleteNamespaceIntegration:
    def test_delete_makes_namespace_unreachable(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]

        del_resp = client.delete(
            f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id}
        )
        assert del_resp.status_code == 202

        assert (
            client.get(
                f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id}
            ).status_code
            == 404
        )

    def test_deleted_namespace_excluded_from_list(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]
        client.delete(f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id})

        resp = client.get("/namespaces", headers={"X-Owner-Id": owner_id})
        assert resp.status_code == 200
        assert all(ns["id"] != ns_id for ns in resp.json())

    def test_delete_missing_namespace_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.delete(
            f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": owner_id}
        )
        assert response.status_code == 404

    def test_double_delete_returns_404(self, client: TestClient, owner_id: str) -> None:
        ns_id = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": owner_id},
        ).json()["id"]
        client.delete(f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id})
        response = client.delete(
            f"/namespaces/{ns_id}", headers={"X-Owner-Id": owner_id}
        )
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
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 202
        task_id = response.json()["task_id"]
        s3_key = f"{ns_id}/{uuid.uuid5(uuid.UUID(ns_id), 'report.pdf')}.pdf"

        import os

        bucket = os.environ["S3_ARTEMIS_BUCKET"]
        stat = test_minio_client.stat_object(bucket, s3_key)
        assert stat.metadata.get("x-amz-meta-task_id") == task_id
        contract = json.loads(stat.metadata.get("x-amz-meta-contract"))
        assert contract["upload_action"] == IngestionTaskType.CREATE

    def test_upload_to_missing_namespace_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.post(
            f"/namespaces/{uuid.uuid4()}/objects",
            files=_file_bytes(),
            headers={"X-Owner-Id": owner_id},
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
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 202
        s3_key = f"{ns_id}/{obj_id}.pdf"

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
            headers={"X-Owner-Id": owner_id},
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
        s3_key = f"{ns_id}/{obj_id}.pdf"

        response = client.delete(
            f"/namespaces/{ns_id}/objects/{obj_id}",
            headers={"X-Owner-Id": owner_id},
        )
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

        response = client.delete(
            f"/namespaces/{ns_id}/objects/{uuid.uuid4()}",
            headers={"X-Owner-Id": owner_id},
        )
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

        response = client.get(
            f"/namespaces/{ns_id}/objects", headers={"X-Owner-Id": owner_id}
        )
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

        resp_a = client.get(
            f"/namespaces/{ns_a}/objects", headers={"X-Owner-Id": owner_id}
        )
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

        response = client.get(
            f"/namespaces/{ns_id}/objects", headers={"X-Owner-Id": owner_id}
        )
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
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.get(
            f"/namespaces/{uuid.uuid4()}/objects", headers={"X-Owner-Id": owner_id}
        )
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
            f"/namespaces/{ns_id}/objects",
            params={"group_id": str(group_id)},
            headers={"X-Owner-Id": owner_id},
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
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 202
        s3_key = f"{ns_id}/{uuid.uuid5(uuid.UUID(ns_id), 'report.pdf')}.pdf"
        stat = test_minio_client.stat_object(os.environ["S3_ARTEMIS_BUCKET"], s3_key)
        contract = json.loads(stat.metadata.get("x-amz-meta-contract"))
        assert contract["info"]["group_id"] == str(group_id)


# ---------------------------------------------------------------------------
# GET /namespaces/{id}/tasks, GET /namespaces/{id}/tasks/{task_id}
#
# Epic 22: both endpoints now read ingestion_status directly instead of the
# retired, terminal-only ingestion_tasks — these seed ingestion_status rows
# directly (standing in for what the worker's outbox would write) rather
# than driving a real Celery pipeline, matching this layer's own contract
# ("can this service read its own infra correctly").
# ---------------------------------------------------------------------------


class TestListTasksIntegration:
    def test_running_task_visible_with_stage(
        self,
        client: TestClient,
        owner_id: str,
        storage_session_factory,
    ) -> None:
        """The exact gap Epic 22 closed: a task still in flight used to have
        no row in the old ingestion_tasks table at all. It must now show up
        here with its live stage and a null completed_at."""
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        task_id = uuid.uuid4()
        _seed_ingestion_status(
            storage_session_factory,
            task_id=task_id,
            namespace_id=ns_id,
            stage="tasks.submit_parse",
            status="running",
        )

        response = client.get(
            f"/namespaces/{ns_id}/tasks", headers={"X-Owner-Id": owner_id}
        )
        assert response.status_code == 200
        tasks = response.json()
        assert len(tasks) == 1
        assert tasks[0]["task_id"] == str(task_id)
        assert tasks[0]["status"] == "running"
        assert tasks[0]["stage"] == "tasks.submit_parse"
        assert tasks[0]["completed_at"] is None

    def test_success_task_has_completed_at(
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
        task_id = uuid.uuid4()
        obj_id = uuid.uuid4()
        _seed_ingestion_status(
            storage_session_factory,
            task_id=task_id,
            namespace_id=ns_id,
            obj_id=obj_id,
            stage="tasks.index",
            status="success",
        )

        response = client.get(
            f"/namespaces/{ns_id}/tasks", headers={"X-Owner-Id": owner_id}
        )
        assert response.status_code == 200
        task = response.json()[0]
        assert task["status"] == "success"
        assert task["obj_id"] == str(obj_id)
        assert task["completed_at"] is not None

    def test_excludes_other_namespace_tasks(
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
        _seed_ingestion_status(
            storage_session_factory, task_id=uuid.uuid4(), namespace_id=ns_a
        )
        _seed_ingestion_status(
            storage_session_factory, task_id=uuid.uuid4(), namespace_id=ns_b
        )

        resp_a = client.get(
            f"/namespaces/{ns_a}/tasks", headers={"X-Owner-Id": owner_id}
        )
        assert len(resp_a.json()) == 1
        assert resp_a.json()[0]["namespace_id"] == str(ns_a)

    def test_missing_namespace_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        response = client.get(
            f"/namespaces/{uuid.uuid4()}/tasks", headers={"X-Owner-Id": owner_id}
        )
        assert response.status_code == 404


class TestGetTaskStatusIntegration:
    def test_running_task_returns_200_not_404(
        self,
        client: TestClient,
        owner_id: str,
        storage_session_factory,
    ) -> None:
        """The exact gap Epic 22 closed: an in-flight task used to 404
        because ingestion_tasks had no row for it yet."""
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        task_id = uuid.uuid4()
        _seed_ingestion_status(
            storage_session_factory,
            task_id=task_id,
            namespace_id=ns_id,
            stage="tasks.poll_chunk",
            status="running",
        )

        response = client.get(
            f"/namespaces/{ns_id}/tasks/{task_id}", headers={"X-Owner-Id": owner_id}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "running"
        assert body["stage"] == "tasks.poll_chunk"
        assert body["completed_at"] is None

    def test_unknown_task_id_returns_404(
        self, client: TestClient, owner_id: str
    ) -> None:
        ns_id = uuid.UUID(
            client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": owner_id},
            ).json()["id"]
        )
        response = client.get(
            f"/namespaces/{ns_id}/tasks/{uuid.uuid4()}",
            headers={"X-Owner-Id": owner_id},
        )
        assert response.status_code == 404

    def test_task_in_other_namespace_returns_404(
        self,
        client: TestClient,
        owner_id: str,
        storage_session_factory,
    ) -> None:
        """ingestion_status has no direct namespace ownership check of its
        own — get_task_status filters on (task_id, namespace_id) together,
        so a task queried under the wrong namespace must 404, not leak."""
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
        task_id = uuid.uuid4()
        _seed_ingestion_status(
            storage_session_factory, task_id=task_id, namespace_id=ns_a
        )

        response = client.get(
            f"/namespaces/{ns_b}/tasks/{task_id}", headers={"X-Owner-Id": owner_id}
        )
        assert response.status_code == 404
