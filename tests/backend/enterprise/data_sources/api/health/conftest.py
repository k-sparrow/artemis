"""Fixtures for data_sources health unit tests."""

from __future__ import annotations

import os

os.environ.setdefault("SQL_DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("STORAGE_SERVICE_URL", "http://localhost:7000")
os.environ.setdefault("KAFKA_CONNECT_URL", "http://localhost:8083")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.backend.enterprise.data_sources.api.main import app  # noqa: E402


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)
