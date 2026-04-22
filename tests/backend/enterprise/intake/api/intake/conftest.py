"""Fixtures for enterprise intake endpoint unit tests.

External dependencies replaced:
  - Storage service   → httpx mock via dependency_overrides (asserted in tests)
  - Filesystem        → tmp_path (pytest built-in)
  - URL fetch         → httpx.AsyncClient patched per-test in service
"""

from __future__ import annotations

import os
import uuid
from typing import Iterator
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("STORAGE_SERVICE_URL", "http://localhost:7000")

import pytest  # noqa: E402
from fastapi import status  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.backend.enterprise.intake.api.dependencies import (  # noqa: E402
    get_storage_client,
)
from src.backend.enterprise.intake.api.main import app  # noqa: E402

_NAMESPACE_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
_TASK_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
_S3_KEY = f"{_NAMESPACE_ID}/some-object-id"

NAMESPACE_RESPONSE = {"id": str(_NAMESPACE_ID), "type": "shared", "name": "acme"}
UPLOAD_RESPONSE = {"task_id": str(_TASK_ID), "s3_key": _S3_KEY}


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
def client(mock_http: MagicMock) -> Iterator[TestClient]:
    app.dependency_overrides[get_storage_client] = lambda: mock_http
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
