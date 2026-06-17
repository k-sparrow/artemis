# Set required env vars before any module-level settings objects are instantiated.
# These are dummy values — all infrastructure dependencies are overridden via
# FastAPI's dependency_overrides in the test fixtures.
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
os.environ.setdefault("PAGE_BUCKET", "test-pages")


@pytest.fixture(autouse=True)
def mock_lifespan_vectorstore(monkeypatch):
    """Prevent the lifespan from attempting a real Qdrant connection.

    The lifespan calls ``get_vectorstore_handler_solved()`` on startup to
    eagerly create and warm up the vectorstore collection.  Unit tests don't
    need real infrastructure, so we replace it with a no-op handler.
    """
    from src.backend.indexing.api import utils

    mock_handler = AsyncMock()
    mock_handler.acreate = AsyncMock(return_value=None)
    mock_handler.aclose = AsyncMock(return_value=None)

    async def _noop():
        return mock_handler

    monkeypatch.setattr(utils, "get_vectorstore_handler_solved", _noop)


@pytest.fixture(autouse=True)
def mock_lifespan_page_store(monkeypatch):
    """Prevent the lifespan's page-bucket ensure from a real MinIO connection.

    The indexing lifespan builds an ``S3ByteStore`` for the parent-page doc store
    and calls ``ensure_bucket`` on startup (mirrors parsing/storage). Unit tests
    have no MinIO, so we patch the builder to a real in-memory ``ByteStore`` (so
    ``NamespaceRetriever``'s pydantic ``ByteStore`` validation passes) with a
    no-op async ``ensure_bucket``.
    """
    from langchain_core.stores import InMemoryByteStore

    from src.backend.indexing.api import utils

    class _FakePageStore(InMemoryByteStore):
        async def ensure_bucket(self) -> None:  # no-op: no MinIO in unit tests
            return None

    monkeypatch.setattr(utils, "build_page_byte_store", lambda: _FakePageStore())
