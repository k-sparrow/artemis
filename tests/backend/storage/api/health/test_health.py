"""Unit tests for the storage service health endpoints.

Liveness: always 200 (no external deps).
Readiness: one check — MinioHealthcheck (bucket_exists call).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.backend.storage.api.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


class TestLiveness:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/health/liveness")
        assert resp.status_code == status.HTTP_200_OK

    def test_body_is_healthy(self, client: TestClient) -> None:
        body = client.get("/health/liveness").json()
        assert body["healthy"] is True
        assert body["checks"] == []


class TestReadiness:
    def test_minio_healthy_returns_200(self, client: TestClient) -> None:
        with patch("minio.Minio.bucket_exists", return_value=True):
            resp = client.get("/health/readiness")
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["healthy"] is True

    def test_minio_bucket_missing_returns_503(self, client: TestClient) -> None:
        with patch("minio.Minio.bucket_exists", return_value=False):
            resp = client.get("/health/readiness")
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert resp.json()["healthy"] is False
