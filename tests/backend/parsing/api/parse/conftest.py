# Set required env vars before any module-level settings objects are instantiated.
# These are dummy values — all infrastructure dependencies are overridden via
# FastAPI's dependency_overrides (and the lifespan S3 client is mocked below).
import os
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DOCLING_SERVE_URI", "http://test-docling:5001")
os.environ.setdefault("S3_ENDPOINT", "test-minio:9000")
os.environ.setdefault("S3_ACCESS_KEY", "test")
os.environ.setdefault("S3_SECRET_KEY", "test")
os.environ.setdefault("S3_SECURE", "false")


@pytest.fixture(autouse=True)
def mock_lifespan_s3(monkeypatch):
    """Prevent the lifespan's artifact-bucket ensure from a real MinIO connection.

    The parsing lifespan calls ``get_s3_client()`` to ensure the artifact bucket
    exists at startup. Unit tests have no MinIO, so we patch it to a no-op client
    (``bucket_exists()`` returns truthy → ``make_bucket`` is never called).
    """
    from src.backend.parsing.api import utils

    monkeypatch.setattr(utils, "get_s3_client", lambda: MagicMock())
