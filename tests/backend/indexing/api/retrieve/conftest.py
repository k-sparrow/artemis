# Set required env vars before any module-level settings objects are instantiated.
# These are dummy values — all infrastructure dependencies are overridden via
# mocks in the test fixtures below.
import os

import pytest
from unittest.mock import AsyncMock


os.environ.setdefault("QDRANT_HOST_URL", "http://test-qdrant:6333")
os.environ.setdefault("QDRANT_COLLECTION_NAME", "test_collection")
os.environ.setdefault("TEI_HOST_URL", "http://test-tei:8080")
os.environ.setdefault("SQL_DB_HOST", "localhost")
os.environ.setdefault("SQL_DB_PORT", "5432")
os.environ.setdefault("SQL_DB_USER", "test")
os.environ.setdefault("SQL_DB_PASSWORD", "test")
os.environ.setdefault("SQL_DB_DATABASE", "test")
os.environ.setdefault("S3_ENDPOINT", "test-minio:9000")
os.environ.setdefault("S3_ACCESS_KEY", "test")
os.environ.setdefault("S3_SECRET_KEY", "test")
os.environ.setdefault("S3_SECURE", "false")


@pytest.fixture(autouse=True)
def mock_lifespan(monkeypatch):
    """Prevent lifespan from attempting real infrastructure connections.

    The lifespan now also initialises the retrieve singleton via
    ``retrieve_service.initialize()``.  We stub both the vectorstore handler
    and the initialise call so that unit and HTTP tests start with a clean,
    un-initialised service state each time.
    """
    from src.backend.indexing.api import utils

    mock_handler = AsyncMock()
    mock_handler.acreate = AsyncMock(return_value=AsyncMock())
    mock_handler.aclose = AsyncMock(return_value=None)

    async def _noop_handler():
        return mock_handler

    monkeypatch.setattr(utils, "get_vectorstore_handler_solved", _noop_handler)
    monkeypatch.setattr(utils.retrieve_service, "initialize", lambda *a, **kw: None)
