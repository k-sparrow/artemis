"""Fixtures for enterprise intake unit tests.

All external I/O is replaced:
  - storage service  → httpx.MockTransport (no real HTTP)
  - filesystem       → tmp_path (pytest built-in)
  - URL fetch        → httpx.MockTransport on the internal client
"""

from __future__ import annotations

import uuid
from typing import Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.backend.enterprise.intake.api.main import app

_NAMESPACE_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
_TASK_ID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
_S3_KEY = f"{_NAMESPACE_ID}/some-object-id"

NAMESPACE_RESPONSE = {"id": str(_NAMESPACE_ID), "type": "shared", "name": "acme"}
UPLOAD_RESPONSE = {"task_id": str(_TASK_ID), "s3_key": _S3_KEY}


@pytest.fixture
def mock_http() -> MagicMock:
    """Async HTTP client mock wired to app.state."""
    client = MagicMock()
    # namespace upsert
    ns_resp = MagicMock()
    ns_resp.status_code = status.HTTP_201_CREATED
    ns_resp.json.return_value = NAMESPACE_RESPONSE
    # object upload
    upload_resp = MagicMock()
    upload_resp.status_code = status.HTTP_202_ACCEPTED
    upload_resp.json.return_value = UPLOAD_RESPONSE

    client.post = AsyncMock(side_effect=[ns_resp, upload_resp])
    return client


@pytest.fixture
def client(mock_http: MagicMock) -> Iterator[TestClient]:
    with TestClient(app) as c:
        app.state.http_client = mock_http
        yield c
