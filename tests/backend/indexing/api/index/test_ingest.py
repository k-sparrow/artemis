# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the /ingest endpoint.

All infrastructure dependencies (pipeline, loader factory) are replaced with
mocks via FastAPI's dependency_overrides — no Qdrant, TEI, Postgres, or
Docling containers are required.
"""

from __future__ import annotations

import uuid
from typing import List
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from langchain_core.documents import Document

from src.backend.indexing.api.dependencies import get_loader_factory, get_pipeline
from src.backend.indexing.api.main import app
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.types import UpsertResult


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def namespace() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def sample_docs() -> List[Document]:
    return [
        Document(
            page_content="chunk one", metadata={"source": "test.pdf", "type": "text"}
        ),
        Document(
            page_content="chunk two", metadata={"source": "test.pdf", "type": "text"}
        ),
    ]


@pytest.fixture
def mock_pipeline(sample_docs: List[Document]) -> AsyncMock:
    pipeline = AsyncMock()
    pipeline.aprocess = AsyncMock(
        return_value=UpsertResult(num_added=2, ids=["id-1", "id-2"])
    )
    return pipeline


@pytest.fixture
def mock_loader_factory(sample_docs: List[Document]) -> MagicMock:
    loader = MagicMock()
    loader.load = MagicMock(return_value=sample_docs)
    factory = MagicMock(return_value=loader)
    return factory


@pytest.fixture
def client(
    mock_pipeline: AsyncMock,
    mock_loader_factory: MagicMock,
):
    """TestClient with all infrastructure dependencies replaced by mocks."""
    app.dependency_overrides[get_pipeline] = lambda: mock_pipeline
    app.dependency_overrides[get_loader_factory] = lambda: mock_loader_factory
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _post_file(
    client: TestClient, namespace: uuid.UUID, content: bytes = b"fake pdf"
) -> ...:
    return client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        files={"file": ("test.pdf", content, "application/pdf")},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIngestEndpoint:
    def test_happy_path_returns_upsert_result(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        """Successful ingestion returns UpsertResult with correct counts and IDs."""
        response = _post_file(client, namespace)

        assert response.status_code == 200
        body = response.json()
        assert body["num_added"] == 2
        assert body["num_skipped"] == 0
        assert body["ids"] == ["id-1", "id-2"]

    def test_loader_httpx_error_returns_503(
        self,
        namespace: uuid.UUID,
        mock_pipeline: AsyncMock,
    ) -> None:
        """An httpx error from the document loader (e.g. Docling down) → 503."""
        failing_loader = MagicMock()
        failing_loader.load = MagicMock(
            side_effect=httpx.ConnectError("connection refused")
        )
        failing_factory = MagicMock(return_value=failing_loader)

        app.dependency_overrides[get_pipeline] = lambda: mock_pipeline
        app.dependency_overrides[get_loader_factory] = lambda: failing_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c, namespace)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 503
        body = response.json()
        assert body["type"] == "upstream_service_error"
        assert body["service"] == "document-loader"

    def test_document_processing_exception_returns_422(
        self,
        namespace: uuid.UUID,
        mock_loader_factory: MagicMock,
    ) -> None:
        """DocumentProcessingException raised by the pipeline → 422."""
        failing_pipeline = AsyncMock()
        failing_pipeline.aprocess = AsyncMock(
            side_effect=DocumentProcessingException("invalid chunk structure")
        )

        app.dependency_overrides[get_pipeline] = lambda: failing_pipeline
        app.dependency_overrides[get_loader_factory] = lambda: mock_loader_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c, namespace)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 422
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "invalid chunk structure" in body["detail"]

    def test_unexpected_pipeline_error_returns_500(
        self,
        namespace: uuid.UUID,
        mock_loader_factory: MagicMock,
    ) -> None:
        """Unhandled exceptions from the pipeline bubble up as 500."""
        crashing_pipeline = AsyncMock()
        crashing_pipeline.aprocess = AsyncMock(
            side_effect=RuntimeError("qdrant connection lost")
        )

        app.dependency_overrides[get_pipeline] = lambda: crashing_pipeline
        app.dependency_overrides[get_loader_factory] = lambda: mock_loader_factory
        try:
            with TestClient(app, raise_server_exceptions=False) as c:
                response = _post_file(c, namespace)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 500

    def test_pipeline_called_with_namespace_in_metadata(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        mock_pipeline: AsyncMock,
    ) -> None:
        """The namespace must be stamped on every document before pipeline.aprocess()."""
        _post_file(client, namespace)

        mock_pipeline.aprocess.assert_called_once()
        docs_arg: List[Document] = mock_pipeline.aprocess.call_args[0][0]
        assert all(
            doc.metadata.get("namespace") == str(namespace) for doc in docs_arg
        ), "All documents must carry the namespace in their metadata"
