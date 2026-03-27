"""Unit tests for GET/POST/PATCH/DELETE /namespaces endpoints.

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

import pytest  # noqa: F401
from fastapi.testclient import TestClient

from src.backend.storage.api.models import NamespaceType
from src.backend.storage.api.namespaces.exceptions import (
    InvalidOwnerIdError,
    OnlyPrivateNamespaceCanBeRenamedError,
)
from src.backend.storage.api.exceptions import NamespaceNotFoundError

_SERVICE = "src.backend.storage.api.namespaces.service"

OWNER_ID = str(uuid.uuid4())


def _namespace(
    type_: NamespaceType = NamespaceType.PRIVATE,
    name: str = "test-ns",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        name=name,
        type=type_,
        owner_id=uuid.UUID(OWNER_ID),
        deleted_at=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# POST /namespaces
# ---------------------------------------------------------------------------


class TestCreateNamespace:
    def test_private_namespace_returns_201(self, client: TestClient) -> None:
        ns = _namespace(NamespaceType.PRIVATE)
        with patch(f"{_SERVICE}.create_namespace", new=AsyncMock(return_value=ns)):
            response = client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 201
        assert response.json()["type"] == "private"

    def test_shared_namespace_with_name_returns_201(self, client: TestClient) -> None:
        ns = _namespace(NamespaceType.SHARED, name="my-project")
        with patch(f"{_SERVICE}.create_namespace", new=AsyncMock(return_value=ns)):
            response = client.post(
                "/namespaces",
                json={"type": "shared", "name": "my-project"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 201
        assert response.json()["name"] == "my-project"

    def test_shared_without_name_returns_422(self, client: TestClient) -> None:
        # Rejected by NamespaceCreate.shared_requires_name model_validator before
        # the service is ever called.
        response = client.post(
            "/namespaces",
            json={"type": "shared"},
            headers={"X-Owner-Id": OWNER_ID},
        )
        assert response.status_code == 422

    def test_invalid_owner_id_returns_400(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.parse_owner_id",
            side_effect=InvalidOwnerIdError(),
        ):
            response = client.post(
                "/namespaces",
                json={"type": "private"},
                headers={"X-Owner-Id": "not-a-uuid"},
            )
        assert response.status_code == 400

    def test_missing_owner_header_returns_422(self, client: TestClient) -> None:
        response = client.post("/namespaces", json={"type": "private"})
        assert response.status_code == 422

    def test_missing_type_returns_422(self, client: TestClient) -> None:
        response = client.post("/namespaces", json={}, headers={"X-Owner-Id": OWNER_ID})
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /namespaces
# ---------------------------------------------------------------------------


class TestListNamespaces:
    def test_returns_list_of_namespaces(self, client: TestClient) -> None:
        namespaces = [_namespace(), _namespace()]
        with patch(
            f"{_SERVICE}.list_namespaces", new=AsyncMock(return_value=namespaces)
        ):
            response = client.get("/namespaces", headers={"X-Owner-Id": OWNER_ID})
        assert response.status_code == 200
        assert len(response.json()) == 2

    def test_empty_result_returns_empty_list(self, client: TestClient) -> None:
        with patch(f"{_SERVICE}.list_namespaces", new=AsyncMock(return_value=[])):
            response = client.get("/namespaces", headers={"X-Owner-Id": OWNER_ID})
        assert response.status_code == 200
        assert response.json() == []

    def test_missing_owner_header_returns_422(self, client: TestClient) -> None:
        response = client.get("/namespaces")
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /namespaces/{namespace_id}
# ---------------------------------------------------------------------------


class TestGetNamespace:
    def test_returns_namespace(self, client: TestClient) -> None:
        ns = _namespace()
        with patch(f"{_SERVICE}.get_namespace", new=AsyncMock(return_value=ns)):
            response = client.get(f"/namespaces/{ns.id}")
        assert response.status_code == 200
        assert response.json()["id"] == str(ns.id)

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_namespace",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.get(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 404

    def test_invalid_namespace_id_returns_422(self, client: TestClient) -> None:
        response = client.get("/namespaces/not-a-uuid")
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# PATCH /namespaces/{namespace_id}
# ---------------------------------------------------------------------------


class TestRenameNamespace:
    def test_renames_and_returns_updated_namespace(self, client: TestClient) -> None:
        ns = _namespace(name="new-name")
        with patch(f"{_SERVICE}.rename_namespace", new=AsyncMock(return_value=ns)):
            response = client.patch(f"/namespaces/{ns.id}", json={"name": "new-name"})
        assert response.status_code == 200
        assert response.json()["name"] == "new-name"

    def test_rename_shared_namespace_returns_409(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.rename_namespace",
            new=AsyncMock(side_effect=OnlyPrivateNamespaceCanBeRenamedError()),
        ):
            response = client.patch(
                f"/namespaces/{uuid.uuid4()}", json={"name": "new-name"}
            )
        assert response.status_code == 409

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.rename_namespace",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.patch(
                f"/namespaces/{uuid.uuid4()}", json={"name": "new-name"}
            )
        assert response.status_code == 404

    def test_missing_name_returns_422(self, client: TestClient) -> None:
        response = client.patch(f"/namespaces/{uuid.uuid4()}", json={})
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# DELETE /namespaces/{namespace_id}
# ---------------------------------------------------------------------------


class TestSoftDeleteNamespace:
    def test_soft_delete_returns_202(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.soft_delete_namespace", new=AsyncMock(return_value=None)
        ):
            response = client.delete(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 202

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.soft_delete_namespace",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.delete(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 404
