import os
import pytest
from unittest.mock import AsyncMock, MagicMock
# Set required env vars before settings objects are instantiated at import time.

os.environ.setdefault("S3_ENDPOINT_URL", "localhost:9000")
os.environ.setdefault("S3_ACCESS_KEY", "test-access-key")
os.environ.setdefault("S3_SECRET_KEY", "test-secret-key")
os.environ.setdefault("S3_ARTEMIS_BUCKET", "test-bucket")
os.environ.setdefault("S3_ARTEMIS_BUCKET_KAFKA_EVENT", "test-event")
os.environ.setdefault("SQL_DB_URL", "postgresql+asyncpg://test:test@localhost/test")


@pytest.fixture(autouse=True)
def mock_lifespan(monkeypatch):
    """Replace the lifespan with a no-op so tests need no real infrastructure.

    main.py binds `lifespan` into `FastAPI(lifespan=lifespan)` at import time,
    so patching `utils.lifespan` has no effect on the already-created `app`.
    FastAPI dispatches startup via `app.router.lifespan_context`, which we
    replace here directly.
    """
    from contextlib import asynccontextmanager
    from src.backend.storage.api.main import app

    @asynccontextmanager
    async def _noop(_app):
        yield

    monkeypatch.setattr(app.router, "lifespan_context", _noop)


@pytest.fixture
def mock_session() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_minio() -> MagicMock:
    return MagicMock()
