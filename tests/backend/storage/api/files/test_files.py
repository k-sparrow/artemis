"""Unit tests for file ingestion and observability endpoints.

All infrastructure (DB, MinIO) is replaced via dependency_overrides.
Service functions are patched so tests exercise only router behaviour:
request parsing, service dispatch, response serialisation, and
exception → HTTP status mapping.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.backend.storage.api.exceptions import (
    NamespaceAccessDeniedError,
    NamespaceNotFoundError,
)
from src.backend.storage.api.files.exceptions import (
    IngestedFileNotFoundError,
    TaskNotFoundError,
)

_SERVICE = "src.backend.storage.api.files.service"

NAMESPACE_ID = uuid.uuid4()
FILE_ID = uuid.uuid4()
TASK_ID = uuid.uuid4()
OWNER_ID = str(uuid.uuid4())
GROUP_ID = uuid.uuid4()

_OWNER_HEADER = {"X-Owner-Id": OWNER_ID}


def _file_bytes(content: bytes = b"hello world") -> dict:
    """Build multipart file payload for TestClient."""
    return {"file": ("report.pdf", content, "application/pdf")}


def _ingested_file_row(group_id: uuid.UUID | None = None) -> dict:
    """Minimal dict that satisfies IngestedObjectResponse.model_validate."""
    return {
        "id": uuid.uuid4(),
        "namespace_id": NAMESPACE_ID,
        "source": "report.pdf",
        "object_type": "file",
        "content_type": "application/pdf",
        "size_bytes": 11,
        "group_id": group_id,
        "ingested_at": datetime(2026, 6, 8, 12, 0, 0, tzinfo=timezone.utc),
    }


def _ingestion_task_row(
    status: str = "SUCCESS", failure_reason: str | None = None
) -> dict:
    """Minimal dict that satisfies IngestionTaskResponse.model_validate."""
    return {
        "task_id": TASK_ID,
        "obj_id": FILE_ID,
        "namespace_id": NAMESPACE_ID,
        "status": status,
        "failure_reason": failure_reason,
        "completed_at": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# POST /namespaces/{namespace_id}/objects
# ---------------------------------------------------------------------------


class TestUploadFile:
    def test_happy_path_returns_202(self, client: TestClient) -> None:
        with patch(f"{_SERVICE}.upload_file", new=AsyncMock(return_value=TASK_ID)):
            response = client.post(
                f"/namespaces/{NAMESPACE_ID}/objects",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 202
        assert response.json()["task_id"] == str(TASK_ID)

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.upload_file",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.post(
                f"/namespaces/{NAMESPACE_ID}/objects",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.upload_file",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.post(
                f"/namespaces/{NAMESPACE_ID}/objects",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.post(
            f"/namespaces/{NAMESPACE_ID}/objects",
            files=_file_bytes(),
        )
        assert response.status_code == 401

    def test_missing_file_body_returns_422(self, client: TestClient) -> None:
        response = client.post(
            f"/namespaces/{NAMESPACE_ID}/objects",
            headers=_OWNER_HEADER,
        )
        assert response.status_code == 422

    def test_invalid_namespace_id_returns_422(self, client: TestClient) -> None:
        response = client.post(
            "/namespaces/not-a-uuid/objects",
            files=_file_bytes(),
            headers=_OWNER_HEADER,
        )
        assert response.status_code == 422

    def test_group_id_query_param_forwarded_to_service(
        self, client: TestClient
    ) -> None:
        with patch(
            f"{_SERVICE}.upload_file", new=AsyncMock(return_value=TASK_ID)
        ) as mock:
            client.post(
                f"/namespaces/{NAMESPACE_ID}/objects",
                files=_file_bytes(),
                params={"group_id": str(GROUP_ID)},
                headers=_OWNER_HEADER,
            )
        mock.assert_awaited_once()
        assert mock.call_args.kwargs["group_id"] == GROUP_ID


# ---------------------------------------------------------------------------
# PUT /namespaces/{namespace_id}/objects/{obj_id}
# ---------------------------------------------------------------------------


class TestReingestFile:
    def test_happy_path_returns_202(self, client: TestClient) -> None:
        with patch(f"{_SERVICE}.reingest_file", new=AsyncMock(return_value=TASK_ID)):
            response = client.put(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 202
        assert response.json()["task_id"] == str(TASK_ID)

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.reingest_file",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.put(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.reingest_file",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.put(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 403

    def test_file_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.reingest_file",
            new=AsyncMock(side_effect=IngestedFileNotFoundError()),
        ):
            response = client.put(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                files=_file_bytes(),
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 404

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.put(
            f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
            files=_file_bytes(),
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# DELETE /namespaces/{namespace_id}/objects/{obj_id}
# ---------------------------------------------------------------------------


class TestDeleteFile:
    def test_happy_path_returns_202(self, client: TestClient) -> None:
        with patch(f"{_SERVICE}.delete_file", new=AsyncMock(return_value=None)):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 202

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.delete_file",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.delete_file",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 403

    def test_file_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.delete_file",
            new=AsyncMock(side_effect=IngestedFileNotFoundError()),
        ):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}",
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 404

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.delete(f"/namespaces/{NAMESPACE_ID}/objects/{FILE_ID}")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# DELETE /namespaces/{namespace_id}/objects  (bulk group delete)
# ---------------------------------------------------------------------------


class TestDeleteGroup:
    def test_happy_path_returns_202_with_task_ids(self, client: TestClient) -> None:
        task_ids = [uuid.uuid4(), uuid.uuid4()]
        with patch(f"{_SERVICE}.delete_group", new=AsyncMock(return_value=task_ids)):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects",
                params={"group_id": str(GROUP_ID)},
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 202
        assert response.json()["task_ids"] == [str(t) for t in task_ids]

    def test_empty_group_returns_202_with_empty_list(self, client: TestClient) -> None:
        with patch(f"{_SERVICE}.delete_group", new=AsyncMock(return_value=[])):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects",
                params={"group_id": str(GROUP_ID)},
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 202
        assert response.json()["task_ids"] == []

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.delete_group",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects",
                params={"group_id": str(GROUP_ID)},
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.delete_group",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.delete(
                f"/namespaces/{NAMESPACE_ID}/objects",
                params={"group_id": str(GROUP_ID)},
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 403

    def test_missing_group_id_returns_422(self, client: TestClient) -> None:
        response = client.delete(
            f"/namespaces/{NAMESPACE_ID}/objects",
            headers=_OWNER_HEADER,
        )
        assert response.status_code == 422

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.delete(
            f"/namespaces/{NAMESPACE_ID}/objects",
            params={"group_id": str(GROUP_ID)},
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# GET /namespaces/{namespace_id}/objects
# ---------------------------------------------------------------------------


class TestListFiles:
    def test_returns_list(self, client: TestClient) -> None:
        file_obj = SimpleNamespace(**_ingested_file_row())

        with patch(f"{_SERVICE}.list_files", new=AsyncMock(return_value=[file_obj])):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/objects", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0]["source"] == "report.pdf"

    def test_null_size_bytes_exposed(self, client: TestClient) -> None:
        file_obj = SimpleNamespace(**_ingested_file_row())
        file_obj.size_bytes = None

        with patch(f"{_SERVICE}.list_files", new=AsyncMock(return_value=[file_obj])):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/objects", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        assert response.json()[0]["size_bytes"] is None

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.list_files",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/objects", headers=_OWNER_HEADER
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.list_files",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/objects", headers=_OWNER_HEADER
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.get(f"/namespaces/{NAMESPACE_ID}/objects")
        assert response.status_code == 401

    def test_group_id_filter_forwarded_to_service(self, client: TestClient) -> None:
        file_obj = SimpleNamespace(**_ingested_file_row(group_id=GROUP_ID))
        with patch(
            f"{_SERVICE}.list_files", new=AsyncMock(return_value=[file_obj])
        ) as mock:
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/objects",
                params={"group_id": str(GROUP_ID)},
                headers=_OWNER_HEADER,
            )
        assert response.status_code == 200
        mock.assert_awaited_once()
        assert mock.call_args.kwargs["group_id"] == GROUP_ID

    def test_group_id_exposed_in_response(self, client: TestClient) -> None:
        file_obj = SimpleNamespace(**_ingested_file_row(group_id=GROUP_ID))
        with patch(f"{_SERVICE}.list_files", new=AsyncMock(return_value=[file_obj])):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/objects", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        assert response.json()[0]["group_id"] == str(GROUP_ID)


# ---------------------------------------------------------------------------
# GET /namespaces/{namespace_id}/tasks
# ---------------------------------------------------------------------------


class TestListTasks:
    def test_returns_list(self, client: TestClient) -> None:
        task_obj = SimpleNamespace(**_ingestion_task_row())

        with patch(f"{_SERVICE}.list_tasks", new=AsyncMock(return_value=[task_obj])):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        assert len(response.json()) == 1

    def test_failed_task_exposes_failure_reason(self, client: TestClient) -> None:
        task_obj = SimpleNamespace(
            **_ingestion_task_row(
                status="FAILURE",
                failure_reason="Traceback...\nValueError: unsupported format",
            )
        )
        task_obj.obj_id = None

        with patch(f"{_SERVICE}.list_tasks", new=AsyncMock(return_value=[task_obj])):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        body = response.json()[0]
        assert body["status"] == "FAILURE"
        assert body["failure_reason"] is not None
        assert body["obj_id"] is None

    def test_empty_namespace_returns_empty_list(self, client: TestClient) -> None:
        with patch(f"{_SERVICE}.list_tasks", new=AsyncMock(return_value=[])):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        assert response.json() == []

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.list_tasks",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks", headers=_OWNER_HEADER
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.list_tasks",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks", headers=_OWNER_HEADER
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.get(f"/namespaces/{NAMESPACE_ID}/tasks")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# GET /namespaces/{namespace_id}/tasks/{task_id}
# ---------------------------------------------------------------------------


class TestGetTaskStatus:
    def test_happy_path_returns_task(self, client: TestClient) -> None:
        task_obj = SimpleNamespace(**_ingestion_task_row())
        with patch(
            f"{_SERVICE}.get_task_status",
            new=AsyncMock(return_value=task_obj),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks/{TASK_ID}", headers=_OWNER_HEADER
            )
        assert response.status_code == 200
        body = response.json()
        assert body["task_id"] == str(TASK_ID)
        assert body["status"] == "SUCCESS"

    def test_namespace_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_task_status",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks/{TASK_ID}", headers=_OWNER_HEADER
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_task_status",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks/{TASK_ID}", headers=_OWNER_HEADER
            )
        assert response.status_code == 403

    def test_task_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_task_status",
            new=AsyncMock(side_effect=TaskNotFoundError()),
        ):
            response = client.get(
                f"/namespaces/{NAMESPACE_ID}/tasks/{TASK_ID}", headers=_OWNER_HEADER
            )
        assert response.status_code == 404

    def test_invalid_task_id_returns_422(self, client: TestClient) -> None:
        response = client.get(
            f"/namespaces/{NAMESPACE_ID}/tasks/not-a-uuid", headers=_OWNER_HEADER
        )
        assert response.status_code == 422

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.get(f"/namespaces/{NAMESPACE_ID}/tasks/{TASK_ID}")
        assert response.status_code == 401
