"""Unit tests for the enterprise intake health endpoints.

Liveness: always 200 (no external deps).
Readiness: three checks — StorageService (HttpCheck), KafkaConnect (HttpCheck),
and HttpSinkConnector (KafkaConnectConnectorCheck).

HttpChecks use aiohttp internally → mocked via aioresponses.
KafkaConnectConnectorCheck uses the module-level kafka_connect_client → patched
by the `client` fixture in conftest.py.
"""

from __future__ import annotations

from kafka_connect import KafkaConnect

import pytest  # noqa: F401
from aioresponses import aioresponses
from fastapi import status
from fastapi.testclient import TestClient

_STORAGE_READINESS_URL = "http://localhost:7000/health/readiness"
_KAFKA_CONNECT_HEALTH_URL = "http://localhost:8083/health"

_RUNNING_STATUS = {
    "connector": {"state": "RUNNING", "worker_id": "worker:8083"},
    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker:8083"}],
}


class TestLiveness:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/health/liveness")
        assert resp.status_code == status.HTTP_200_OK

    def test_body_is_healthy(self, client: TestClient) -> None:
        body = client.get("/health/liveness").json()
        assert body["healthy"] is True
        assert body["checks"] == []


class TestReadiness:
    def test_all_healthy_returns_200(
        self, client: TestClient, mock_kc: KafkaConnect
    ) -> None:
        mock_kc.get_connector_status.return_value = _RUNNING_STATUS

        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=200)
            m.get(_KAFKA_CONNECT_HEALTH_URL, status=200)
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["healthy"] is True

    def test_storage_down_returns_503(
        self, client: TestClient, mock_kc: KafkaConnect
    ) -> None:
        mock_kc.get_connector_status.return_value = _RUNNING_STATUS

        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=503)
            m.get(_KAFKA_CONNECT_HEALTH_URL, status=200)
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_kafka_connect_down_returns_503(
        self, client: TestClient, mock_kc: KafkaConnect
    ) -> None:
        mock_kc.get_connector_status.return_value = _RUNNING_STATUS

        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=200)
            m.get(_KAFKA_CONNECT_HEALTH_URL, status=503)
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_connector_not_running_returns_503(
        self, client: TestClient, mock_kc: KafkaConnect
    ) -> None:
        mock_kc.get_connector_status.return_value = {
            "connector": {"state": "FAILED", "worker_id": "worker:8083"},
            "tasks": [
                {
                    "id": 0,
                    "state": "FAILED",
                    "worker_id": "worker:8083",
                    "trace": "java.lang.Exception",
                }
            ],
        }

        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=200)
            m.get(_KAFKA_CONNECT_HEALTH_URL, status=200)
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_connector_no_tasks_returns_503(
        self, client: TestClient, mock_kc: KafkaConnect
    ) -> None:
        mock_kc.get_connector_status.return_value = {
            "connector": {"state": "RUNNING", "worker_id": "worker:8083"},
            "tasks": [],
        }

        with aioresponses() as m:
            m.get(_STORAGE_READINESS_URL, status=200)
            m.get(_KAFKA_CONNECT_HEALTH_URL, status=200)
            resp = client.get("/health/readiness")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
