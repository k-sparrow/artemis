"""Unit tests for the enterprise intake health endpoints.

Liveness: always 200 (no external deps).
Readiness: delegates to the storage service — mocked via aiohttp patching.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.backend.enterprise.intake.api.main import app

_HTTP_CHECK_MODULE = "fastapi_healthchecks.checks.http"


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _make_session_mock(ok: bool, http_status: int) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.ok = ok
    mock_resp.status = http_status

    mock_get_ctx = MagicMock()
    mock_get_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_get_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    mock_session.get = MagicMock(return_value=mock_get_ctx)

    return mock_session


class TestLiveness:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/health/liveness")
        assert resp.status_code == status.HTTP_200_OK

    def test_body_is_healthy(self, client: TestClient) -> None:
        body = client.get("/health/liveness").json()
        assert body["healthy"] is True
        assert body["checks"] == []


class TestReadiness:
    def test_storage_healthy_returns_200(self, client: TestClient) -> None:
        mock_session = _make_session_mock(ok=True, http_status=status.HTTP_200_OK)

        with patch(
            f"{_HTTP_CHECK_MODULE}.ClientSession", return_value=mock_session
        ), patch(f"{_HTTP_CHECK_MODULE}.TCPConnector"):
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_200_OK
        assert client.get("/health/readiness").json()  # body is valid JSON

    def test_storage_unhealthy_returns_503(self, client: TestClient) -> None:
        mock_session = _make_session_mock(
            ok=False, http_status=status.HTTP_503_SERVICE_UNAVAILABLE
        )

        with patch(
            f"{_HTTP_CHECK_MODULE}.ClientSession", return_value=mock_session
        ), patch(f"{_HTTP_CHECK_MODULE}.TCPConnector"):
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
