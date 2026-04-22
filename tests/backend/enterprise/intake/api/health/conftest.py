"""Fixtures for enterprise intake health unit tests.

External dependencies replaced:
  - Storage service DI → get_storage_client overridden
"""

from __future__ import annotations

import os
from typing import Iterator
from unittest.mock import MagicMock

os.environ.setdefault("STORAGE_SERVICE_URL", "http://localhost:7000")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.backend.enterprise.intake.api.dependencies import (  # noqa: E402
    get_storage_client,
)
from src.backend.enterprise.intake.api.main import app  # noqa: E402


@pytest.fixture
def client() -> Iterator[TestClient]:
    app.dependency_overrides[get_storage_client] = lambda: MagicMock()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
