"""Unit tests for POST /v1/parse/submit|status|resolve|finalize endpoints.

Covers the four new async-path endpoints added in Epic 18.  The primary gap
is the batch code path through /v1/parse/submit (large PDF sharding → scratch
bucket upload → submit_batch call), which is never exercised in integration
tests because the e2e test documents are small.  These tests are the only
thing standing between a sharding bug and production.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    get_blob_store_factory,
    get_docling_client,
    get_s3_client,
)
from src.backend.parsing.api.main import app
from src.backend.parsing.lib.artifact import ParseStatus
from src.lib.core.adapters.stores.memory.blob import InMemoryBlobStore

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")
OBJ_ID_STR = str(OBJ_ID)
CONV_TASK_ID = "stub-conv-task-id"
CHUNK_TASK_ID = "stub-chunk-task-id"

_CHUNK_ITEM = {"text": "hello world", "filename": "doc.pdf", "page_numbers": [1]}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _docling_doc_json() -> bytes:
    from docling.datamodel.document import DoclingDocument

    return DoclingDocument(name="test").model_dump_json().encode()


def _mock_docling_client() -> MagicMock:
    """MagicMock for DoclingParseClient with AsyncMock methods."""
    client = MagicMock()
    client.submit_file = AsyncMock(return_value=CONV_TASK_ID)
    client.submit_batch = AsyncMock(return_value=CONV_TASK_ID)
    client.get_status = AsyncMock(
        return_value=ParseStatus(status="success", num_processed=1, num_total=1)
    )
    # fetch_conversion_result returns a mock DoclingDocument (model_dump_json called
    # by resolve to build the replay cache bytes).
    mock_dl_doc = MagicMock()
    mock_dl_doc.model_dump_json.return_value = _docling_doc_json().decode()
    client.fetch_conversion_result = AsyncMock(return_value=mock_dl_doc)
    client.submit_chunk = AsyncMock(return_value=CHUNK_TASK_ID)
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
    def test_non_pdf_takes_single_path(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """Non-PDF content → submit_file called, mode=single in response."""
        resp = client.post(
            "/v1/parse/submit",
            files={"file": ("report.md", b"# Hello", "text/markdown")},
            data={"metadata": _metadata()},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "single"
        assert body["parsing_task_id"] == CONV_TASK_ID
        assert body["shard_count"] is None
        docling_client.submit_file.assert_awaited_once()
        docling_client.submit_batch.assert_not_awaited()

    def test_pdf_below_threshold_takes_single_path(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """PDF with page count ≤ threshold → single-file path, no shards created."""
        small_page_count = settings.DOCLING_SHARD_TRIGGER_PAGES - 1
        with patch(
            "src.backend.parsing.api.parse.router.pdf_page_count",
            return_value=small_page_count,
        ):
            resp = client.post(
                "/v1/parse/submit",
                files={"file": ("small.pdf", b"%PDF-1.4", "application/pdf")},
                data={"metadata": _metadata()},
            )
        assert resp.status_code == 200
        assert resp.json()["mode"] == "single"
        docling_client.submit_file.assert_awaited_once()
        docling_client.submit_batch.assert_not_awaited()

    def test_large_pdf_takes_batch_path(
        self,
        client: TestClient,
        docling_client: MagicMock,
    ) -> None:
        """PDF exceeding threshold → submit_batch called, mode=batch in response."""
        with (
            patch(
                "src.backend.parsing.api.parse.router.pdf_page_count",
                return_value=settings.DOCLING_SHARD_TRIGGER_PAGES + 1,
            ),
            patch(
                "src.backend.parsing.api.parse.router.split_pdf_shards",
                return_value=[b"shard-0", b"shard-1"],
            ),
        ):
            resp = client.post(
                "/v1/parse/submit",
                files={"file": ("large.pdf", b"%PDF-1.4", "application/pdf")},
                data={"metadata": _metadata()},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "batch"
        assert body["parsing_task_id"] == CONV_TASK_ID
        docling_client.submit_file.assert_not_awaited()
        docling_client.submit_batch.assert_awaited_once()

    def test_batch_mode_shard_count_matches_split_output(
        self,
        client: TestClient,
        docling_client: MagicMock,
    ) -> None:
        """
        shard_count in response equals the number of shards split_pdf_shards returned.
        """
        fake_shards = [b"s0", b"s1", b"s2"]
        with (
            patch(
                "src.backend.parsing.api.parse.router.pdf_page_count",
                return_value=settings.DOCLING_SHARD_TRIGGER_PAGES + 1,
            ),
            patch(
                "src.backend.parsing.api.parse.router.split_pdf_shards",
                return_value=fake_shards,
            ),
        ):
            resp = client.post(
                "/v1/parse/submit",
                files={"file": ("large.pdf", b"%PDF-1.4", "application/pdf")},
                data={"metadata": _metadata()},
            )
        assert resp.json()["shard_count"] == len(fake_shards)

    def test_batch_mode_uploads_shards_to_scratch_with_correct_keys(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """Each shard lands at {obj_id}/pages/shard-{n:04d}.pdf in scratch bucket."""
        fake_shards = [b"shard-bytes-0", b"shard-bytes-1"]
        with (
            patch(
                "src.backend.parsing.api.parse.router.pdf_page_count",
                return_value=settings.DOCLING_SHARD_TRIGGER_PAGES + 1,
            ),
            patch(
                "src.backend.parsing.api.parse.router.split_pdf_shards",
                return_value=fake_shards,
            ),
        ):
            resp = client.post(
                "/v1/parse/submit",
                files={"file": ("large.pdf", b"%PDF-1.4", "application/pdf")},
                data={"metadata": _metadata()},
            )
        assert resp.status_code == 200
        assert store.get(f"{OBJ_ID_STR}/pages/shard-0000.pdf") == b"shard-bytes-0"
        assert store.get(f"{OBJ_ID_STR}/pages/shard-0001.pdf") == b"shard-bytes-1"

    def test_batch_mode_scratch_prefix_contains_obj_id(
        self,
        client: TestClient,
        docling_client: MagicMock,
    ) -> None:
        """scratch_prefix in response is scoped to the obj_id."""
        with (
            patch(
                "src.backend.parsing.api.parse.router.pdf_page_count",
                return_value=settings.DOCLING_SHARD_TRIGGER_PAGES + 1,
            ),
            patch(
                "src.backend.parsing.api.parse.router.split_pdf_shards",
                return_value=[b"s"],
            ),
        ):
            resp = client.post(
                "/v1/parse/submit",
                files={"file": ("large.pdf", b"%PDF-1.4", "application/pdf")},
                data={"metadata": _metadata()},
            )
        assert OBJ_ID_STR in resp.json()["scratch_prefix"]

    def test_source_ref_path_reads_from_store(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """
        source_ref form field → content fetched from blob store, then submit_file called.
        """
        store.put("ingest-source/doc.md", b"# content")
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
        assert resp.json()["mode"] == "single"
        docling_client.submit_file.assert_awaited_once()

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
    def _resolve_body(self, mode: str = "single") -> dict:
        return {
            "parsing_task_id": CONV_TASK_ID,
            "mode": mode,
            "obj_id": OBJ_ID_STR,
            "scratch_prefix": f"{OBJ_ID_STR}/" if mode == "batch" else None,
            "shard_count": 2 if mode == "batch" else None,
        }

    def test_single_mode_fetches_conversion_result(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """Single mode calls fetch_conversion_result with the parsing_task_id."""
        resp = client.post("/v1/parse/resolve", json=self._resolve_body("single"))
        assert resp.status_code == 200
        docling_client.fetch_conversion_result.assert_awaited_once_with(
            CONV_TASK_ID, timeout=settings.DOCLING_RESOLVE_TIMEOUT
        )

    def test_single_mode_writes_replay_cache_and_submits_chunk(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """
        After resolve: replay cache written, chunk job submitted,
        response has chunking_task_id.
        """
        resp = client.post("/v1/parse/resolve", json=self._resolve_body("single"))
        assert resp.status_code == 200
        assert resp.json() == {"chunking_task_id": CHUNK_TASK_ID, "obj_id": OBJ_ID_STR}
        # Replay cache must be written before chunk submission.
        assert store.exists(f"replay/{OBJ_ID_STR}.json")
        docling_client.submit_chunk.assert_awaited_once()

    def test_batch_mode_reads_shards_from_scratch_and_concatenates(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """Batch mode: shard JSONs read from scratch bucket, replay cache written,
        chunk job submitted, chunking_task_id returned."""
        from docling.datamodel.document import DoclingDocument

        # docling_jobkit stores results at {dst_prefix}{hash}/{shard}.json;
        # simulate the hash subdirectory that the router must absorb.
        shard_key = f"{OBJ_ID_STR}/results/abc123def456/shard-0000.json"
        store.put(shard_key, DoclingDocument(name="shard").model_dump_json().encode())

        mock_obj = MagicMock()
        mock_obj.object_name = shard_key
        mock_s3 = MagicMock()
        mock_s3.list_objects.return_value = [mock_obj]
        app.dependency_overrides[get_s3_client] = lambda: mock_s3

        try:
            resp = client.post("/v1/parse/resolve", json=self._resolve_body("batch"))
        finally:
            app.dependency_overrides.pop(get_s3_client, None)

        assert resp.status_code == 200
        assert resp.json() == {"chunking_task_id": CHUNK_TASK_ID, "obj_id": OBJ_ID_STR}
        assert store.exists(f"replay/{OBJ_ID_STR}.json")
        docling_client.submit_chunk.assert_awaited_once()

    def test_batch_mode_returns_502_when_no_shard_results(
        self,
        client: TestClient,
        docling_client: MagicMock,
    ) -> None:
        """Batch mode with no results in scratch → 502."""
        mock_s3 = MagicMock()
        mock_s3.list_objects.return_value = []
        app.dependency_overrides[get_s3_client] = lambda: mock_s3
        try:
            resp = client.post("/v1/parse/resolve", json=self._resolve_body("batch"))
        finally:
            app.dependency_overrides.pop(get_s3_client, None)
        assert resp.status_code == 502


# ---------------------------------------------------------------------------
# POST /v1/parse/finalize
# ---------------------------------------------------------------------------


class TestFinalizeEndpoint:
    def _finalize_body(self) -> dict:
        return {
            "chunking_task_id": CHUNK_TASK_ID,
            "obj_id": OBJ_ID_STR,
            "metadata": {"obj_id": OBJ_ID_STR},
        }

    @pytest.fixture(autouse=True)
    def seed_replay_cache(self, store: InMemoryBlobStore) -> None:
        """Finalize reads the replay cache written by resolve; pre-populate it."""
        store.put(f"replay/{OBJ_ID_STR}.json", _docling_doc_json())

    def test_returns_artifact_blob_ref(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.post("/v1/parse/finalize", json=self._finalize_body())
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
        resp = client.post("/v1/parse/finalize", json=self._finalize_body())
        assert resp.status_code == 200
        artifact_bytes = store.get(f"parse/{OBJ_ID_STR}.json")
        artifact = json.loads(artifact_bytes)
        assert "chunks" in artifact
        assert "pages" in artifact
        assert artifact["chunks"][0]["page_content"] == "hello world"

    def test_fetch_chunk_result_called_with_correct_task_id(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        client.post("/v1/parse/finalize", json=self._finalize_body())
        docling_client.fetch_chunk_result.assert_awaited_once_with(
            CHUNK_TASK_ID, timeout=settings.DOCLING_FINALIZE_TIMEOUT
        )
