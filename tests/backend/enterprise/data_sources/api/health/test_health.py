"""Unit tests for the data_sources health endpoints."""

from __future__ import annotations

from aioresponses import aioresponses
from fastapi import status
from fastapi.testclient import TestClient

_STORAGE_READINESS_URL = "http://localhost:7000/health/readiness"
_KAFKA_CONNECT_URL = "http://localhost:8083/"


class TestLiveness:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/health/liveness")
        assert resp.status_code == status.HTTP_200_OK

    def test_body_is_healthy(self, client: TestClient) -> None:
        body = client.get("/health/liveness").json()
        assert body["healthy"] is True
        assert body["checks"] == []


class TestReadiness:
    def test_both_healthy_returns_200(self, client: TestClient) -> None:
        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=200)
            m.get(_KAFKA_CONNECT_URL, status=200)
            resp = client.get("/health/readiness")
        assert resp.status_code == status.HTTP_200_OK

    def test_storage_unhealthy_returns_503(self, client: TestClient) -> None:
        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=503)
            m.get(_KAFKA_CONNECT_URL, status=200)
            resp = client.get("/health/readiness")
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_kafka_connect_unhealthy_returns_503(self, client: TestClient) -> None:
        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=200)
            m.get(_KAFKA_CONNECT_URL, status=503)
            resp = client.get("/health/readiness")
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
