"""Unit tests for the /data-sources endpoints.

All external I/O (KafkaConnect, storage service, Postgres) is mocked — see conftest.py.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest  # noqa: F401
from fastapi import status
from fastapi.testclient import TestClient
from requests.exceptions import HTTPError

from src.backend.enterprise.data_sources.api.dependencies import (
    get_db_session,
    get_storage_client,
)
from src.backend.enterprise.data_sources.api.main import app
from src.backend.enterprise.data_sources.api.models import Base
from src.lib.backend.db.session import get_session
from tests.backend.enterprise.data_sources.api.sources.conftest import _NAMESPACE_ID

import asyncio  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402, E501

_CREATE_PAYLOAD = {
    "display_name": "Acme Docs",
    "path": "/data/acme",
    "namespace": "acme",
    "org_name": "acme-corp",
}


async def _create_all(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def _make_client(mock_kc, mock_storage=None):
    """Helper: build a fresh TestClient with given mocks and isolated SQLite DB."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    asyncio.get_event_loop().run_until_complete(_create_all(engine))
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def _get_session():
        async for s in get_session(factory):
            yield s

    return engine, factory, _get_session


class TestCreateDataSource:
    def test_returns_201(self, client: TestClient) -> None:
        resp = client.post("/data-sources", json=_CREATE_PAYLOAD)
        assert resp.status_code == status.HTTP_201_CREATED

    def test_response_shape(self, client: TestClient) -> None:
        body = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        assert "id" in body
        assert body["display_name"] == "Acme Docs"
        assert body["source_type"] == "filesystem"
        assert body["connector_name"].startswith("artemis-")
        assert body["org_name"] == "acme-corp"
        assert body["config"] == {"path": "/data/acme"}

    def test_kafka_status_included(self, client: TestClient) -> None:
        body = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        assert body["kafka_status"]["state"] == "RUNNING"

    def test_namespace_upserted_on_storage_service(
        self, client: TestClient, mock_storage_client: MagicMock
    ) -> None:
        client.post("/data-sources", json=_CREATE_PAYLOAD)
        mock_storage_client.post.assert_called_once()
        call_kwargs = mock_storage_client.post.call_args
        assert call_kwargs[0][0] == "/namespaces"
        assert call_kwargs[1]["json"]["type"] == "shared"
        assert call_kwargs[1]["json"]["name"] == "acme"

    def test_connector_deployed_to_kafka_connect(
        self, client: TestClient, mock_kafka_connect: MagicMock
    ) -> None:
        client.post("/data-sources", json=_CREATE_PAYLOAD)
        mock_kafka_connect.create_connector.assert_called_once()
        config = mock_kafka_connect.create_connector.call_args[0][0]
        assert config["name"].startswith("artemis-")
        assert config["config"]["camel.source.path.directoryName"] == "/data/acme"
        assert config["config"]["camel.source.endpoint.noop"] == "true"
        ns_id_literal = config["config"][
            "transforms.InjectNamespaceIdHeader.value.literal"
        ]
        assert ns_id_literal == str(_NAMESPACE_ID)

    def test_namespace_409_resolves_existing(
        self, mock_kafka_connect: MagicMock, db_factory
    ) -> None:
        existing_ns_id = uuid.uuid5(
            uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "acme"
        )

        conflict_resp = MagicMock()
        conflict_resp.status_code = status.HTTP_409_CONFLICT
        conflict_resp.json.return_value = {
            "id": str(existing_ns_id),
            "type": "shared",
            "name": "acme",
        }

        conflict_client = MagicMock()
        conflict_client.post = AsyncMock(return_value=conflict_resp)
        conflict_client.delete = AsyncMock()

        engine, _, _get_session = _make_client(mock_kafka_connect)

        with patch(
            "src.backend.enterprise.data_sources.api.sources.service.KafkaConnect",
            return_value=mock_kafka_connect,
        ):
            app.dependency_overrides[get_db_session] = _get_session
            app.dependency_overrides[get_storage_client] = lambda: conflict_client
            with TestClient(app) as c:
                resp = c.post("/data-sources", json=_CREATE_PAYLOAD)
            app.dependency_overrides.clear()

        engine.sync_engine.dispose()
        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()["namespace_id"] == str(existing_ns_id)

    def test_kafka_connect_error_returns_502(
        self, mock_storage_client: MagicMock, db_factory
    ) -> None:
        failing_kc = MagicMock()
        failing_kc.create_connector.side_effect = HTTPError("500 Server Error")
        failing_kc.get_connector_status.return_value = {
            "connector": {"state": "RUNNING", "worker_id": "w"},
            "tasks": [],
        }

        engine, _, _get_session = _make_client(failing_kc)

        with patch(
            "src.backend.enterprise.data_sources.api.sources.service.KafkaConnect",
            return_value=failing_kc,
        ):
            app.dependency_overrides[get_db_session] = _get_session
            app.dependency_overrides[get_storage_client] = lambda: mock_storage_client
            with TestClient(app) as c:
                resp = c.post("/data-sources", json=_CREATE_PAYLOAD)
            app.dependency_overrides.clear()

        engine.sync_engine.dispose()
        assert resp.status_code == status.HTTP_502_BAD_GATEWAY

    def test_storage_service_error_returns_502(
        self, mock_kafka_connect: MagicMock, db_factory
    ) -> None:
        error_resp = MagicMock()
        error_resp.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        error_resp.text = "Internal Server Error"

        failing_storage = MagicMock()
        failing_storage.post = AsyncMock(return_value=error_resp)

        engine, _, _get_session = _make_client(mock_kafka_connect)

        with patch(
            "src.backend.enterprise.data_sources.api.sources.service.KafkaConnect",
            return_value=mock_kafka_connect,
        ):
            app.dependency_overrides[get_db_session] = _get_session
            app.dependency_overrides[get_storage_client] = lambda: failing_storage
            with TestClient(app) as c:
                resp = c.post("/data-sources", json=_CREATE_PAYLOAD)
            app.dependency_overrides.clear()

        engine.sync_engine.dispose()
        assert resp.status_code == status.HTTP_502_BAD_GATEWAY

    def test_missing_display_name_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/data-sources",
            json={"path": "/data/acme", "namespace": "acme", "org_name": "acme-corp"},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_missing_path_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/data-sources",
            json={"display_name": "Acme", "namespace": "acme", "org_name": "acme-corp"},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestListDataSources:
    def test_empty_returns_200_empty_list(self, client: TestClient) -> None:
        resp = client.get("/data-sources")
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json() == []

    def test_returns_created_sources(self, client: TestClient) -> None:
        client.post("/data-sources", json=_CREATE_PAYLOAD)
        resp = client.get("/data-sources")
        assert resp.status_code == status.HTTP_200_OK
        assert len(resp.json()) == 1
        assert resp.json()[0]["display_name"] == "Acme Docs"


class TestGetDataSource:
    def test_returns_200(self, client: TestClient) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        resp = client.get(f"/data-sources/{created['id']}")
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["id"] == created["id"]

    def test_unknown_id_returns_404(self, client: TestClient) -> None:
        resp = client.get(f"/data-sources/{uuid.uuid4()}")
        assert resp.status_code == status.HTTP_404_NOT_FOUND


class TestDeleteDataSource:
    def test_returns_204(
        self, client: TestClient, mock_kafka_connect: MagicMock
    ) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        resp = client.delete(f"/data-sources/{created['id']}")
        assert resp.status_code == status.HTTP_204_NO_CONTENT

    def test_deleted_source_no_longer_listed(self, client: TestClient) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        client.delete(f"/data-sources/{created['id']}")
        assert client.get("/data-sources").json() == []

    def test_connector_removed_from_kafka_connect(
        self, client: TestClient, mock_kafka_connect: MagicMock
    ) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        client.delete(f"/data-sources/{created['id']}")
        mock_kafka_connect.delete_connector.assert_called_once_with(
            created["connector_name"]
        )

    def test_namespace_hard_deleted_on_last_connector_delete(
        self, client: TestClient, mock_storage_client: MagicMock
    ) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        client.delete(f"/data-sources/{created['id']}")
        # No siblings: single DELETE to namespace (tombstones objects + hard-deletes row).
        # Group-scoped delete is skipped when the connector is the last one.
        assert mock_storage_client.delete.call_count == 1
        call_url = mock_storage_client.delete.call_args[0][0]
        assert call_url == f"/namespaces/{created['namespace_id']}"

    def test_unknown_id_returns_404(self, client: TestClient) -> None:
        resp = client.delete(f"/data-sources/{uuid.uuid4()}")
        assert resp.status_code == status.HTTP_404_NOT_FOUND


class TestPauseResumeRestart:
    def test_pause_returns_200(
        self, client: TestClient, mock_kafka_connect: MagicMock
    ) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        resp = client.post(f"/data-sources/{created['id']}/pause")
        assert resp.status_code == status.HTTP_200_OK
        mock_kafka_connect.pause_connector.assert_called_once_with(
            created["connector_name"]
        )

    def test_resume_returns_200(
        self, client: TestClient, mock_kafka_connect: MagicMock
    ) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        resp = client.post(f"/data-sources/{created['id']}/resume")
        assert resp.status_code == status.HTTP_200_OK
        mock_kafka_connect.resume_connector.assert_called_once_with(
            created["connector_name"]
        )

    def test_restart_returns_200(
        self, client: TestClient, mock_kafka_connect: MagicMock
    ) -> None:
        created = client.post("/data-sources", json=_CREATE_PAYLOAD).json()
        resp = client.post(f"/data-sources/{created['id']}/restart")
        assert resp.status_code == status.HTTP_200_OK
        mock_kafka_connect.restart_connector.assert_called_once_with(
            created["connector_name"], include_tasks=True
        )

    def test_pause_unknown_id_returns_404(self, client: TestClient) -> None:
        resp = client.post(f"/data-sources/{uuid.uuid4()}/pause")
        assert resp.status_code == status.HTTP_404_NOT_FOUND
