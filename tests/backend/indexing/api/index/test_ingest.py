# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the POST /ingest and DELETE /ingest endpoints.

POST /ingest is inline-or-reference: the body is an ``IngestRequest`` carrying
either inline ``chunks`` or an ``artifact_ref`` BlobRef (read from object
storage). The pipeline and the blob-store factory are replaced via
dependency_overrides — an in-memory store backs the artifact-read path, so no
Qdrant/TEI/Postgres/MinIO is required.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.backend.indexing.api.dependencies import (
    get_blob_store_factory,
    get_pipeline,
)
from src.backend.indexing.api.main import app
from src.lib.core.adapters.stores.memory.blob import InMemoryBlobStore
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.pipeline.parent_page import PagedInput
from src.lib.core.ingestion.types import UpsertResult

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _chunk(content: str = "hello world", obj_id: uuid.UUID = OBJ_ID) -> dict:
    return {
        "page_content": content,
        "source": "test.md",
        "type": "text",
        "obj_id": str(obj_id),
    }


def _page(markdown: str, page_no: int = 1, obj_id: uuid.UUID = OBJ_ID) -> dict:
    return {"obj_id": str(obj_id), "page_no": page_no, "markdown": markdown}


def _artifact_bytes(chunks: list[dict], pages: list[dict] | None = None) -> bytes:
    """Encode a ParseArtifact (pages + chunks) as the parsing service would."""
    return json.dumps({"pages": pages or [], "chunks": chunks}).encode()


def _paged_arg(mock_pipeline: AsyncMock) -> PagedInput:
    """The :class:`PagedInput` the service handed the (wrapped) pipeline."""
    return mock_pipeline.aprocess.call_args[0][0]


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
def store() -> InMemoryBlobStore:
    return InMemoryBlobStore()


@pytest.fixture
def make_client(store: InMemoryBlobStore):
    @contextmanager
    def _make(pipeline, *, raise_server_exceptions: bool = True):
        app.dependency_overrides[get_pipeline] = lambda: pipeline
        app.dependency_overrides[get_blob_store_factory] = lambda: (lambda _b: store)
        try:
            with TestClient(
                app, raise_server_exceptions=raise_server_exceptions
            ) as client:
                yield client
        finally:
            app.dependency_overrides.clear()

    return _make


@pytest.fixture
def client(make_client, mock_pipeline: AsyncMock):
    with make_client(mock_pipeline) as c:
        yield c


def _delete(client: TestClient, namespace: uuid.UUID, obj_id: str | None = None):
    params: dict = {"namespace": str(namespace)}
    if obj_id is not None:
        params["obj_id"] = obj_id
    return client.delete("/ingest", params=params)


def _post_chunks(
    client: TestClient,
    namespace: uuid.UUID,
    chunks: list | None = None,
    group_id: str | None = None,
):
    if chunks is None:
        chunks = [_chunk()]
    params: dict = {"namespace": str(namespace)}
    if group_id is not None:
        params["group_id"] = group_id
    return client.post("/ingest", params=params, json={"chunks": chunks})


def _post_artifact_ref(client: TestClient, namespace: uuid.UUID, bucket: str, key: str):
    return client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"artifact_ref": {"bucket": bucket, "key": key}},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIngestEndpoint:
    def test_inline_chunks_returns_upsert_result(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        response = _post_chunks(client, namespace)

        assert response.status_code == 200
        body = response.json()
        assert body["num_added"] == 2
        assert body["num_skipped"] == 0
        assert body["ids"] == ["id-1", "id-2"]

    def test_artifact_ref_reads_store_then_indexes(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        store: InMemoryBlobStore,
        mock_pipeline: AsyncMock,
    ) -> None:
        """artifact_ref path: the ParseArtifact is read by key; BOTH its chunks
        and its page parents reach the pipeline as a ``PagedInput``."""
        store.put(
            "parsed-chunks/x.json",
            _artifact_bytes(
                [_chunk("from artifact")],
                pages=[_page("# from artifact")],
            ),
        )
        response = _post_artifact_ref(
            client, namespace, bucket="parsed-chunks", key="parsed-chunks/x.json"
        )

        assert response.status_code == 200
        paged = _paged_arg(mock_pipeline)
        assert [d.page_content for d in paged.chunks] == ["from artifact"]
        assert [d.page_content for d in paged.pages] == ["# from artifact"]
        assert paged.pages[0].metadata["obj_id"] == str(OBJ_ID)

    def test_requires_exactly_one_input(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        """Neither chunks nor artifact_ref → 422."""
        response = client.post("/ingest", params={"namespace": str(namespace)}, json={})
        assert response.status_code == 422

    def test_empty_inline_chunks_returns_422_and_pipeline_never_called(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        """chunks=[] is NOT the same as omitting chunks — it must never reach
        the pipeline as PagedInput(pages=[], chunks=[]), which reads as a
        full namespace wipe (the same class of bug as the artifact_ref
        guards, just via the other input mode)."""
        response = client.post(
            "/ingest", params={"namespace": str(namespace)}, json={"chunks": []}
        )

        assert response.status_code == 422
        body = response.json()
        assert body["type"] == "document_processing_error"
        mock_pipeline.aprocess.assert_not_called()

    def test_missing_required_chunk_field_returns_422(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        response = _post_chunks(
            client,
            namespace,
            chunks=[{"source": "test.md", "type": "text", "obj_id": str(OBJ_ID)}],
        )
        assert response.status_code == 422

    def test_document_processing_exception_returns_422(
        self, make_client, namespace: uuid.UUID
    ) -> None:
        failing = AsyncMock()
        failing.aprocess = AsyncMock(
            side_effect=DocumentProcessingException("invalid chunk structure")
        )
        with make_client(failing) as c:
            response = _post_chunks(c, namespace)

        assert response.status_code == 422
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "invalid chunk structure" in body["detail"]

    def test_unexpected_pipeline_error_returns_500(
        self, make_client, namespace: uuid.UUID
    ) -> None:
        crashing = AsyncMock()
        crashing.aprocess = AsyncMock(
            side_effect=RuntimeError("qdrant connection lost")
        )
        with make_client(crashing, raise_server_exceptions=False) as c:
            response = _post_chunks(c, namespace)

        assert response.status_code == 500

    def test_namespace_and_obj_id_stamped_on_docs(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        _post_chunks(client, namespace)

        mock_pipeline.aprocess.assert_called_once()
        docs_arg = _paged_arg(mock_pipeline).chunks
        assert all(
            doc.metadata.get("namespace_id") == str(namespace) for doc in docs_arg
        )
        assert all(doc.metadata.get("obj_id") == str(OBJ_ID) for doc in docs_arg)

    def test_group_id_stamped_when_provided(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        group_id = str(uuid.uuid4())
        _post_chunks(client, namespace, group_id=group_id)

        docs_arg = _paged_arg(mock_pipeline).chunks
        assert all(doc.metadata.get("group_id") == group_id for doc in docs_arg)

    def test_group_id_absent_when_not_provided(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        _post_chunks(client, namespace)

        docs_arg = _paged_arg(mock_pipeline).chunks
        assert all("group_id" not in doc.metadata for doc in docs_arg)

    def test_multiple_chunks_all_passed_to_pipeline(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        chunks = [_chunk(f"chunk {i}") for i in range(5)]
        _post_chunks(client, namespace, chunks=chunks)

        assert len(_paged_arg(mock_pipeline).chunks) == 5


class TestArtifactGuards:
    """Guards on the artifact_ref path against the empty-chunk RecordManager
    namespace wipe (a single object degrading to zero chunks must never reach
    the pipeline with an empty batch — see ParentPagePipeline.aprocess)."""

    def test_zero_pages_returns_422_and_pipeline_never_called(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        store: InMemoryBlobStore,
        mock_pipeline: AsyncMock,
    ) -> None:
        store.put("parsed-chunks/zero-pages.json", _artifact_bytes([_chunk()], pages=[]))

        response = _post_artifact_ref(
            client, namespace, bucket="parsed-chunks", key="parsed-chunks/zero-pages.json"
        )

        assert response.status_code == 422
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "zero pages" in body["detail"]
        mock_pipeline.aprocess.assert_not_called()

    def test_zero_chunks_whitespace_only_pages_returns_422_after_fallback(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        store: InMemoryBlobStore,
        mock_pipeline: AsyncMock,
    ) -> None:
        store.put(
            "parsed-chunks/whitespace.json",
            _artifact_bytes([], pages=[_page("   \n\n   ")]),
        )

        response = _post_artifact_ref(
            client, namespace, bucket="parsed-chunks", key="parsed-chunks/whitespace.json"
        )

        assert response.status_code == 422
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "fallback page-split" in body["detail"]
        mock_pipeline.aprocess.assert_not_called()

    def test_zero_chunks_real_page_content_falls_back_and_succeeds(
        self,
        client: TestClient,
        namespace: uuid.UUID,
        store: InMemoryBlobStore,
        mock_pipeline: AsyncMock,
    ) -> None:
        store.put(
            "parsed-chunks/fallback.json",
            _artifact_bytes(
                [],
                pages=[_page("# Real heading\n\nSome real body content to split.")],
            ),
        )

        response = _post_artifact_ref(
            client, namespace, bucket="parsed-chunks", key="parsed-chunks/fallback.json"
        )

        assert response.status_code == 200
        mock_pipeline.aprocess.assert_called_once()
        paged = _paged_arg(mock_pipeline)
        assert len(paged.chunks) >= 1
        assert all(chunk.metadata["obj_id"] == str(OBJ_ID) for chunk in paged.chunks)


class TestDeleteEndpoint:
    def test_single_object_deletion_returns_204(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        assert _delete(client, namespace, obj_id=str(OBJ_ID)).status_code == 204

    def test_single_object_deletion_calls_adelete_source(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        _delete(client, namespace, obj_id=str(OBJ_ID))
        mock_pipeline.adelete_source.assert_called_once_with(str(OBJ_ID))

    def test_namespace_deletion_returns_204(
        self, client: TestClient, namespace: uuid.UUID
    ) -> None:
        assert _delete(client, namespace).status_code == 204

    def test_namespace_deletion_calls_aprocess_with_empty_paged_input(
        self, client: TestClient, namespace: uuid.UUID, mock_pipeline: AsyncMock
    ) -> None:
        _delete(client, namespace)
        # Empty PagedInput is the namespace-wipe signal to the wrapped pipeline.
        mock_pipeline.aprocess.assert_called_once_with(PagedInput())

    def test_missing_namespace_returns_422(self, client: TestClient) -> None:
        assert client.delete("/ingest").status_code == 422

    def test_adelete_source_error_returns_500(
        self, make_client, namespace: uuid.UUID
    ) -> None:
        crashing = AsyncMock()
        crashing.adelete_source = AsyncMock(
            side_effect=RuntimeError("qdrant unavailable")
        )
        with make_client(crashing, raise_server_exceptions=False) as c:
            response = _delete(c, namespace, obj_id=str(OBJ_ID))

        assert response.status_code == 500
