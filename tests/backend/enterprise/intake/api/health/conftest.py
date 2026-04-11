"""Fixtures for enterprise intake health unit tests.

External dependencies replaced:
  - KafkaConnect lifespan upsert   → module-level kafka_connect_client patched in utils
  - KafkaConnect readiness check   → module-level kafka_connect_client patched in health.router    # noqa: E501
  - Storage service DI             → get_storage_client overridden (not asserted in health tests)  # noqa: E501
"""

from __future__ import annotations

import os
from typing import Iterator
from unittest.mock import MagicMock, patch

os.environ.setdefault("STORAGE_SERVICE_URL", "http://localhost:7000")
os.environ.setdefault("KAFKA_CONNECT_URL", "http://localhost:8083")
os.environ.setdefault("HTTP_SINK_CONNECTOR_NAME", "artemis-enterprise-http-sink")
os.environ.setdefault("INTAKE_URL", "http://localhost:9000/intake")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.backend.enterprise.intake.api.dependencies import (  # noqa: E402
    get_storage_client,
    kafka_connect_client,
)
from src.backend.enterprise.intake.api.main import app  # noqa: E402

_RUNNING_STATUS = {
    "connector": {"state": "RUNNING", "worker_id": "worker:8083"},
    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker:8083"}],
}


@pytest.fixture
def mock_kc() -> MagicMock:
    """Patches methods on the real kafka_connect_client instance.

    KafkaConnectConnectorCheck captures a reference to the object at import
    time, so patching the module-level name has no effect — we must mock the
    methods on the object itself.
    """
    with patch.object(
        kafka_connect_client, "update_connector", return_value={}
    ), patch.object(
        kafka_connect_client, "get_connector_status", return_value=_RUNNING_STATUS
    ):
        yield kafka_connect_client


@pytest.fixture
def client(mock_kc: MagicMock) -> Iterator[TestClient]:
    app.dependency_overrides[get_storage_client] = lambda: MagicMock()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
