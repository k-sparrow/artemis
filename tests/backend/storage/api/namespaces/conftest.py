import pytest
from typing import Iterator
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient

from src.backend.storage.api.main import app
from src.backend.storage.api.dependencies import get_db_session, get_minio_client


@pytest.fixture
def client(mock_session: AsyncMock, mock_minio: MagicMock) -> Iterator[TestClient]:
    app.dependency_overrides[get_db_session] = lambda: mock_session
    app.dependency_overrides[get_minio_client] = lambda: mock_minio
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
