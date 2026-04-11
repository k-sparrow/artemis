"""Fixtures for enterprise intake endpoint unit tests.

External dependencies replaced:
  - Storage service   → httpx mock via dependency_overrides (asserted in tests)
  - KafkaConnect      → module-level kafka_connect_client patched in utils (lifespan only)
  - Filesystem        → tmp_path (pytest built-in)
  - URL fetch         → httpx.AsyncClient patched per-test in service
"""

from __future__ import annotations

import os
import uuid
from typing import Iterator
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("STORAGE_SERVICE_URL", "http://localhost:7000")
os.environ.setdefault("KAFKA_CONNECT_URL", "http://localhost:8083")
os.environ.setdefault("HTTP_SINK_CONNECTOR_NAME", "artemis-enterprise-http-sink")
os.environ.setdefault("INTAKE_URL", "http://localhost:9000/intake")

import pytest  # noqa: E402
from fastapi import status  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.backend.enterprise.intake.api.dependencies import (  # noqa: E402
    get_storage_client,
    kafka_connect_client,
)
from src.backend.enterprise.intake.api.main import app  # noqa: E402

_NAMESPACE_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
_TASK_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
_S3_KEY = f"{_NAMESPACE_ID}/some-object-id"

NAMESPACE_RESPONSE = {"id": str(_NAMESPACE_ID), "type": "shared", "name": "acme"}
UPLOAD_RESPONSE = {"task_id": str(_TASK_ID), "s3_key": _S3_KEY}

_RUNNING_STATUS = {
    "connector": {"state": "RUNNING", "worker_id": "worker:8083"},
    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker:8083"}],
}


@pytest.fixture
def mock_kc() -> MagicMock:
    """Patches methods on the real kafka_connect_client instance.

    The lifespan in utils.py imports kafka_connect_client directly, so patching
    the module-level name has no effect — we must mock the methods on the object.
    """
    with patch.object(
        kafka_connect_client, "update_connector", return_value={}
    ), patch.object(
        kafka_connect_client, "get_connector_status", return_value=_RUNNING_STATUS
    ):
        yield kafka_connect_client


@pytest.fixture
def mock_http() -> MagicMock:
    client = MagicMock()
    ns_resp = MagicMock()
    ns_resp.status_code = status.HTTP_200_OK
    ns_resp.json.return_value = NAMESPACE_RESPONSE
    upload_resp = MagicMock()
    upload_resp.status_code = status.HTTP_202_ACCEPTED
    upload_resp.json.return_value = UPLOAD_RESPONSE
    client.get = AsyncMock(return_value=ns_resp)
    client.post = AsyncMock(return_value=upload_resp)
    return client


@pytest.fixture
def client(mock_http: MagicMock, mock_kc: MagicMock) -> Iterator[TestClient]:
    app.dependency_overrides[get_storage_client] = lambda: mock_http
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
