"""Unit tests for POST /v1/parse/submit|status|resolve and /v1/chunk/submit|status|finalize.

Covers the async-path endpoints (Epic 18). Large-PDF handling is no longer a
client-side concern here — docling-serve's Ray engine fans large PDFs into
page slices and converts them concurrently server-side (Epic 21), so every
document takes the same single-file submit/resolve path regardless of size.

Chunking is a fully decoupled stage from conversion (Epic 21) — /v1/parse/resolve
only downloads the conversion result, caches it (replay JSON + derived pages),
and returns — it no longer submits a chunk job. /v1/chunk/submit, .../status,
and .../finalize own that separately, submitting the replay-cached
DoclingDocument to docling-serve directly from S3 (mirrors /v1/parse/submit's
source_ref path) rather than re-uploading it.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    get_blob_store_factory,
    get_docling_client,
)
from src.backend.parsing.api.main import app
from src.backend.parsing.api.parse import service as parse_service
from src.backend.parsing.lib.artifact import ParseStatus
from src.lib.core.adapters.stores.memory.blob import InMemoryBlobStore
from src.lib.core.ingestion.types import Page

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")
OBJ_ID_STR = str(OBJ_ID)
CONV_TASK_ID = "stub-conv-task-id"
CHUNK_TASK_ID = "stub-chunk-task-id"

_CHUNK_ITEM = {"text": "hello world", "filename": "doc.pdf", "page_numbers": [1]}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_docling_doc():
    """Real (not mocked) empty DoclingDocument — resolve calls real methods on
    it (model_dump_json, export_to_markdown, .pages) via build_pages/split_pages,
    which a MagicMock can't stand in for."""
    from docling.datamodel.document import DoclingDocument

    return DoclingDocument(name="test")


def _mock_docling_client() -> MagicMock:
    """MagicMock for DoclingParseClient with AsyncMock methods."""
    client = MagicMock()
    client.submit_file = AsyncMock(return_value=CONV_TASK_ID)
    client.submit_source = AsyncMock(return_value=CONV_TASK_ID)
    client.get_status = AsyncMock(
        return_value=ParseStatus(status="success", num_processed=1, num_total=1)
    )
    client.fetch_conversion_result = AsyncMock(return_value=_empty_docling_doc())
    client.submit_chunk = AsyncMock(return_value=CHUNK_TASK_ID)
    client.submit_chunk_source = AsyncMock(return_value=CHUNK_TASK_ID)
    client.fetch_chunk_result = AsyncMock(return_value=[_CHUNK_ITEM])
    return client


def _metadata(obj_id: uuid.UUID = OBJ_ID) -> str:
    return json.dumps({"obj_id": str(obj_id)})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store() -> InMemoryBlobStore:
    """Single in-memory store standing in for all MinIO buckets."""
    return InMemoryBlobStore()


@pytest.fixture
def docling_client() -> MagicMock:
    return _mock_docling_client()


@pytest.fixture
def client(store: InMemoryBlobStore, docling_client: MagicMock) -> TestClient:
    app.dependency_overrides[get_docling_client] = lambda: docling_client
    app.dependency_overrides[get_blob_store_factory] = lambda: (lambda _b: store)
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# POST /v1/parse/submit
# ---------------------------------------------------------------------------


class TestSubmitEndpoint:
    def test_non_pdf_submits_single_file(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.post(
            "/v1/parse/submit",
            files={"file": ("report.md", b"# Hello", "text/markdown")},
            data={"metadata": _metadata()},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["parsing_task_id"] == CONV_TASK_ID
        assert body["obj_id"] == OBJ_ID_STR
        docling_client.submit_file.assert_awaited_once()

    def test_large_pdf_still_submits_single_file(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """No client-side page-count branching — docling-serve's Ray engine
        fans large PDFs out server-side (Epic 21)."""
        resp = client.post(
            "/v1/parse/submit",
            files={"file": ("large.pdf", b"%PDF-1.4", "application/pdf")},
            data={"metadata": _metadata()},
        )
        assert resp.status_code == 200
        assert resp.json()["parsing_task_id"] == CONV_TASK_ID
        docling_client.submit_file.assert_awaited_once()

    def test_source_ref_path_calls_submit_source(
        self,
        client: TestClient,
        docling_client: MagicMock,
    ) -> None:
        """
        source_ref form field → docling_client.submit_source called with the
        ref's bucket/key; docling-serve fetches directly from S3/MinIO, this
        service never reads the bytes itself (Epic 21).
        """
        resp = client.post(
            "/v1/parse/submit",
            data={
                "source_ref": json.dumps(
                    {"bucket": "ingest-source", "key": "ingest-source/doc.md"}
                ),
                "filename": "doc.md",
                "content_type": "text/markdown",
                "metadata": _metadata(),
            },
        )
        assert resp.status_code == 200
        docling_client.submit_source.assert_awaited_once_with(
            s3_endpoint=settings.S3_ENDPOINT,
            s3_access_key=settings.S3_ACCESS_KEY,
            s3_secret_key=settings.S3_SECRET_KEY,
            s3_secure=settings.S3_SECURE,
            bucket="ingest-source",
            key="ingest-source/doc.md",
            timeout=120.0,
        )
        docling_client.submit_file.assert_not_awaited()

    def test_returns_422_when_neither_file_nor_source_ref(
        self, client: TestClient
    ) -> None:
        resp = client.post(
            "/v1/parse/submit",
            data={"metadata": _metadata()},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /v1/parse/status/{task_id}
# ---------------------------------------------------------------------------


class TestStatusEndpoint:
    def test_returns_status_from_docling_client(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.get(f"/v1/parse/status/{CONV_TASK_ID}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"
        docling_client.get_status.assert_awaited_once_with(
            CONV_TASK_ID, timeout=settings.DOCLING_STATUS_TIMEOUT
        )

    def test_processing_status_forwarded(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.get_status = AsyncMock(
            return_value=ParseStatus(status="processing")
        )
        resp = client.get(f"/v1/parse/status/{CONV_TASK_ID}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "processing"

    def test_failure_status_forwarded(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.get_status = AsyncMock(
            return_value=ParseStatus(
                status="failure", error_message="conversion failed"
            )
        )
        resp = client.get(f"/v1/parse/status/{CONV_TASK_ID}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "failure"
        assert body["error_message"] == "conversion failed"


# ---------------------------------------------------------------------------
# POST /v1/parse/resolve
# ---------------------------------------------------------------------------


class TestResolveEndpoint:
    def _resolve_body(self) -> dict:
        return {"parsing_task_id": CONV_TASK_ID, "obj_id": OBJ_ID_STR}

    def test_fetches_conversion_result(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.post("/v1/parse/resolve", json=self._resolve_body())
        assert resp.status_code == 200
        docling_client.fetch_conversion_result.assert_awaited_once_with(
            CONV_TASK_ID, timeout=settings.DOCLING_RESOLVE_TIMEOUT
        )

    def test_writes_replay_and_pages_cache_without_submitting_chunk(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """
        After resolve: replay cache AND pages cache written, response is just
        the obj_id — chunking is a fully separate stage now (Epic 21), resolve
        never submits a chunk job itself.
        """
        resp = client.post("/v1/parse/resolve", json=self._resolve_body())
        assert resp.status_code == 200
        assert resp.json() == {"obj_id": OBJ_ID_STR}
        assert store.exists(f"replay/{OBJ_ID_STR}.json")
        assert store.exists(f"pages/{OBJ_ID_STR}.json")
        docling_client.submit_chunk_source.assert_not_awaited()
        docling_client.submit_chunk.assert_not_awaited()


# ---------------------------------------------------------------------------
# POST /v1/chunk/submit
# ---------------------------------------------------------------------------


class TestChunkSubmitEndpoint:
    def test_submits_replay_cache_from_s3(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """
        /v1/chunk/submit hands docling-serve the replay-cache S3 location —
        it fetches the JSON itself, this service never re-reads or
        re-uploads it (mirrors /v1/parse/submit's source_ref path).
        """
        resp = client.post("/v1/chunk/submit", json={"obj_id": OBJ_ID_STR})
        assert resp.status_code == 200
        assert resp.json() == {
            "chunking_task_id": CHUNK_TASK_ID,
            "obj_id": OBJ_ID_STR,
        }
        docling_client.submit_chunk_source.assert_awaited_once_with(
            s3_endpoint=settings.S3_ENDPOINT,
            s3_access_key=settings.S3_ACCESS_KEY,
            s3_secret_key=settings.S3_SECRET_KEY,
            s3_secure=settings.S3_SECURE,
            bucket=settings.REPLAY_CACHE_BUCKET,
            key=f"replay/{OBJ_ID_STR}.json",
            timeout=120.0,
        )


# ---------------------------------------------------------------------------
# GET /v1/chunk/status/{task_id}
# ---------------------------------------------------------------------------


class TestChunkStatusEndpoint:
    def test_returns_status_from_docling_client(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.get(f"/v1/chunk/status/{CHUNK_TASK_ID}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"
        docling_client.get_status.assert_awaited_once_with(
            CHUNK_TASK_ID, timeout=settings.DOCLING_STATUS_TIMEOUT
        )

    def test_failure_status_forwarded(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.get_status = AsyncMock(
            return_value=ParseStatus(status="failure", error_message="chunking failed")
        )
        resp = client.get(f"/v1/chunk/status/{CHUNK_TASK_ID}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "failure"
        assert body["error_message"] == "chunking failed"


# ---------------------------------------------------------------------------
# POST /v1/chunk/finalize
# ---------------------------------------------------------------------------


class TestChunkFinalizeEndpoint:
    def _finalize_body(self) -> dict:
        return {
            "chunking_task_id": CHUNK_TASK_ID,
            "obj_id": OBJ_ID_STR,
            "metadata": {"obj_id": OBJ_ID_STR},
        }

    @pytest.fixture(autouse=True)
    def seed_pages_cache(self, store: InMemoryBlobStore) -> None:
        """Chunk finalize reads the pages cache /v1/parse/resolve writes —
        never re-derives pages from the DoclingDocument itself — so this
        seeds it directly, standing in for a prior resolve call."""
        pages = [Page(obj_id=OBJ_ID, page_no=1, markdown="# Seeded Page")]
        store.put(f"pages/{OBJ_ID_STR}.json", parse_service.encode_pages(pages))

    def test_returns_artifact_blob_ref(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.post("/v1/chunk/finalize", json=self._finalize_body())
        assert resp.status_code == 200
        body = resp.json()
        assert body["bucket"] == settings.PARSED_ARTIFACTS_BUCKET
        assert body["key"] == f"parse/{OBJ_ID_STR}.json"

    def test_artifact_written_to_store(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        resp = client.post("/v1/chunk/finalize", json=self._finalize_body())
        assert resp.status_code == 200
        artifact_bytes = store.get(f"parse/{OBJ_ID_STR}.json")
        artifact = json.loads(artifact_bytes)
        assert "chunks" in artifact
        assert "pages" in artifact
        assert artifact["chunks"][0]["page_content"] == "hello world"
        # Pages come from the cache seed_pages_cache wrote, not re-derived here.
        assert artifact["pages"][0]["markdown"] == "# Seeded Page"

    def test_fetch_chunk_result_called_with_correct_task_id(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        client.post("/v1/chunk/finalize", json=self._finalize_body())
        docling_client.fetch_chunk_result.assert_awaited_once_with(
            CHUNK_TASK_ID, timeout=settings.DOCLING_FINALIZE_TIMEOUT
        )
