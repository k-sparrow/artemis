# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the POST /ingest and DELETE /ingest endpoints.

The indexing service accepts pre-parsed List[ParsedChunk] JSON — it no longer
handles file uploads or calls any document loader.  All infrastructure
dependencies (pipeline) are replaced with mocks via FastAPI's
dependency_overrides — no Qdrant, TEI, or Postgres containers are required.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.backend.indexing.api.dependencies import get_pipeline
from src.backend.indexing.api.main import app
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.types import UpsertResult

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def namespace() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def mock_pipeline() -> AsyncMock:
    pipeline = AsyncMock()
    pipeline.aprocess = AsyncMock(
        return_value=UpsertResult(num_added=2, ids=["id-1", "id-2"])
    )
    pipeline.adelete_source = AsyncMock(return_value=None)
    return pipeline


@pytest.fixture
def client(mock_pipeline: AsyncMock):
    """TestClient with the pipeline replaced by a mock."""
    app.dependency_overrides[get_pipeline] = lambda: mock_pipeline
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _delete(
    client: TestClient,
    namespace: uuid.UUID,
    obj_id: str | None = None,
):
    params: dict = {"namespace": str(namespace)}
    if obj_id is not None:
        params["obj_id"] = obj_id
    return client.delete("/ingest", params=params)


def _post_chunks(
    client: TestClient,
    namespace: uuid.UUID,
    chunks: list | None = None,
    obj_id: uuid.UUID = OBJ_ID,
):
    if chunks is None:
        chunks = [
            {
                "page_content": "hello world",
                "source": "test.md",
                "type": "text",
                "obj_id": str(obj_id),
            }
        ]
    return client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json=chunks,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIngestEndpoint:
    def test_happy_path_returns_upsert_result(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        """Successful ingestion returns UpsertResult with correct counts and IDs."""
        response = _post_chunks(client, namespace)

        assert response.status_code == 200
        body = response.json()
        assert body["num_added"] == 2
        assert body["num_skipped"] == 0
        assert body["ids"] == ["id-1", "id-2"]

    def test_missing_required_chunk_field_returns_422(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        """Chunks missing required fields are rejected by Pydantic validation → 422."""
        response = _post_chunks(
            client,
            namespace,
            chunks=[
                {"source": "test.md", "type": "text", "obj_id": str(OBJ_ID)}
            ],  # page_content missing
        )
        assert response.status_code == 422

    def test_document_processing_exception_returns_422(
        self,
        namespace: uuid.UUID,
    ) -> None:
        """DocumentProcessingException raised by the pipeline → 422."""
        failing_pipeline = AsyncMock()
        failing_pipeline.aprocess = AsyncMock(
            side_effect=DocumentProcessingException("invalid chunk structure")
        )

        app.dependency_overrides[get_pipeline] = lambda: failing_pipeline
        try:
            with TestClient(app) as c:
                response = _post_chunks(c, namespace)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 422
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "invalid chunk structure" in body["detail"]

    def test_unexpected_pipeline_error_returns_500(
        self,
        namespace: uuid.UUID,
    ) -> None:
        """Unhandled exceptions from the pipeline bubble up as 500."""
        crashing_pipeline = AsyncMock()
        crashing_pipeline.aprocess = AsyncMock(
            side_effect=RuntimeError("qdrant connection lost")
        )

        app.dependency_overrides[get_pipeline] = lambda: crashing_pipeline
        try:
            with TestClient(app, raise_server_exceptions=False) as c:
                response = _post_chunks(c, namespace)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 500

    def test_pipeline_called_with_namespace_and_obj_id_in_metadata(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        mock_pipeline: AsyncMock,
    ) -> None:
        """
        Both namespace and obj_id must be stamped on
        every document before pipeline.aprocess().
        """
        _post_chunks(client, namespace)

        mock_pipeline.aprocess.assert_called_once()
        docs_arg = mock_pipeline.aprocess.call_args[0][0]
        assert all(
            doc.metadata.get("namespace_id") == str(namespace) for doc in docs_arg
        ), "All documents must carry the namespace_id in their metadata"
        assert all(
            doc.metadata.get("obj_id") == str(OBJ_ID) for doc in docs_arg
        ), "All documents must carry obj_id in their metadata"

    def test_multiple_chunks_all_passed_to_pipeline(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        mock_pipeline: AsyncMock,
    ) -> None:
        """All chunks in the request body are forwarded to the pipeline."""
        chunks = [
            {
                "page_content": f"chunk {i}",
                "source": "test.md",
                "type": "text",
                "obj_id": str(OBJ_ID),
            }
            for i in range(5)
        ]
        _post_chunks(client, namespace, chunks=chunks)

        docs_arg = mock_pipeline.aprocess.call_args[0][0]
        assert len(docs_arg) == 5


class TestDeleteEndpoint:
    def test_single_object_deletion_returns_204(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        """DELETE /ingest?namespace=<uuid>&obj_id=<uuid> → 204 No Content."""
        response = _delete(client, namespace, obj_id=str(OBJ_ID))
        assert response.status_code == 204

    def test_single_object_deletion_calls_adelete_source(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        mock_pipeline: AsyncMock,
    ) -> None:
        """pipeline.adelete_source must be called with the exact obj_id string."""
        _delete(client, namespace, obj_id=str(OBJ_ID))
        mock_pipeline.adelete_source.assert_called_once_with(str(OBJ_ID))

    def test_namespace_deletion_returns_204(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        """DELETE /ingest?namespace=<uuid> (no obj_id) → 204 No Content."""
        response = _delete(client, namespace)
        assert response.status_code == 204

    def test_namespace_deletion_calls_aprocess_with_empty_list(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        mock_pipeline: AsyncMock,
    ) -> None:
        """Namespace deletion must delegate to pipeline.aprocess([])."""
        _delete(client, namespace)
        mock_pipeline.aprocess.assert_called_once_with([])

    def test_missing_namespace_returns_422(self, client: TestClient) -> None:
        """DELETE /ingest without namespace query param → 422."""
        response = client.delete("/ingest")
        assert response.status_code == 422

    def test_adelete_source_error_returns_500(self, namespace: uuid.UUID) -> None:
        """Unhandled exception from adelete_source bubbles up as 500."""
        crashing_pipeline = AsyncMock()
        crashing_pipeline.adelete_source = AsyncMock(
            side_effect=RuntimeError("qdrant unavailable")
        )

        app.dependency_overrides[get_pipeline] = lambda: crashing_pipeline
        try:
            with TestClient(app, raise_server_exceptions=False) as c:
                response = _delete(c, namespace, obj_id=str(OBJ_ID))
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 500
