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
    NamespaceAlreadyExistsError,
    OnlyPrivateNamespaceCanBeRenamedError,
)
from src.backend.storage.api.exceptions import (
    NamespaceAccessDeniedError,
    NamespaceNotFoundError,
)

_SERVICE = "src.backend.storage.api.namespaces.service"
_ROUTER = "src.backend.storage.api.namespaces.router"

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
        response = client.post(
            "/namespaces",
            json={"type": "shared"},
            headers={"X-Owner-Id": OWNER_ID},
        )
        assert response.status_code == 422

    def test_duplicate_shared_namespace_returns_409(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.create_namespace",
            side_effect=NamespaceAlreadyExistsError(
                {
                    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "name": "acme",
                    "type": "shared",
                }
            ),
        ):
            response = client.post(
                "/namespaces",
                json={"type": "shared", "name": "acme"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 409

    def test_invalid_owner_id_returns_401(self, client: TestClient) -> None:
        response = client.post(
            "/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": "not-a-uuid"},
        )
        assert response.status_code == 401

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.post("/namespaces", json={"type": "private"})
        assert response.status_code == 401

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

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.get("/namespaces")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# GET /namespaces/by-name/{name}
# ---------------------------------------------------------------------------


class TestGetNamespaceByName:
    def test_returns_namespace(self, client: TestClient) -> None:
        ns = _namespace(NamespaceType.SHARED, name="acme")
        with patch(f"{_SERVICE}.get_namespace_by_name", new=AsyncMock(return_value=ns)):
            response = client.get(
                "/namespaces/by-name/acme", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 200
        assert response.json()["name"] == "acme"

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_namespace_by_name",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.get(
                "/namespaces/by-name/missing", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_namespace_by_name",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.get(
                "/namespaces/by-name/acme", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.get("/namespaces/by-name/acme")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# GET /namespaces/{namespace_id}
# ---------------------------------------------------------------------------


class TestGetNamespace:
    def test_returns_namespace(self, client: TestClient) -> None:
        ns = _namespace()
        with patch(f"{_SERVICE}.get_namespace", new=AsyncMock(return_value=ns)):
            response = client.get(
                f"/namespaces/{ns.id}", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 200
        assert response.json()["id"] == str(ns.id)

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_namespace",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.get(
                f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.get_namespace",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.get(
                f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.get(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 401

    def test_invalid_namespace_id_returns_422(self, client: TestClient) -> None:
        response = client.get(
            "/namespaces/not-a-uuid", headers={"X-Owner-Id": OWNER_ID}
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# PATCH /namespaces/{namespace_id}
# ---------------------------------------------------------------------------


class TestRenameNamespace:
    def test_renames_and_returns_updated_namespace(self, client: TestClient) -> None:
        ns = _namespace(name="new-name")
        with patch(f"{_SERVICE}.rename_namespace", new=AsyncMock(return_value=ns)):
            response = client.patch(
                f"/namespaces/{ns.id}",
                json={"name": "new-name"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 200
        assert response.json()["name"] == "new-name"

    def test_rename_shared_namespace_returns_409(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.rename_namespace",
            new=AsyncMock(side_effect=OnlyPrivateNamespaceCanBeRenamedError()),
        ):
            response = client.patch(
                f"/namespaces/{uuid.uuid4()}",
                json={"name": "new-name"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 409

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.rename_namespace",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.patch(
                f"/namespaces/{uuid.uuid4()}",
                json={"name": "new-name"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_SERVICE}.rename_namespace",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.patch(
                f"/namespaces/{uuid.uuid4()}",
                json={"name": "new-name"},
                headers={"X-Owner-Id": OWNER_ID},
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.patch(
            f"/namespaces/{uuid.uuid4()}", json={"name": "new-name"}
        )
        assert response.status_code == 401

    def test_missing_name_returns_422(self, client: TestClient) -> None:
        response = client.patch(
            f"/namespaces/{uuid.uuid4()}",
            json={},
            headers={"X-Owner-Id": OWNER_ID},
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# DELETE /namespaces/{namespace_id}
# ---------------------------------------------------------------------------


class TestDeleteNamespace:
    def test_delete_returns_202(self, client: TestClient) -> None:
        with patch(
            f"{_ROUTER}.tombstone_objects", new=AsyncMock(return_value=[])
        ), patch(f"{_SERVICE}.hard_delete_namespace", new=AsyncMock(return_value=None)):
            response = client.delete(
                f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 202

    def test_not_found_returns_404(self, client: TestClient) -> None:
        with patch(
            f"{_ROUTER}.tombstone_objects", new=AsyncMock(return_value=[])
        ), patch(
            f"{_SERVICE}.hard_delete_namespace",
            new=AsyncMock(side_effect=NamespaceNotFoundError()),
        ):
            response = client.delete(
                f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 404

    def test_access_denied_returns_403(self, client: TestClient) -> None:
        with patch(
            f"{_ROUTER}.tombstone_objects", new=AsyncMock(return_value=[])
        ), patch(
            f"{_SERVICE}.hard_delete_namespace",
            new=AsyncMock(side_effect=NamespaceAccessDeniedError()),
        ):
            response = client.delete(
                f"/namespaces/{uuid.uuid4()}", headers={"X-Owner-Id": OWNER_ID}
            )
        assert response.status_code == 403

    def test_missing_owner_header_returns_401(self, client: TestClient) -> None:
        response = client.delete(f"/namespaces/{uuid.uuid4()}")
        assert response.status_code == 401
