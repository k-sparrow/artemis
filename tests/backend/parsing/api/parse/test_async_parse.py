"""
Unit tests for POST /v1/parse/submit|status|resolve and
/v1/chunk/submit|status|finalize.

Covers the async-path endpoints (Epic 18). Large-PDF handling is no longer a
client-side concern here — docling-serve's Ray engine fans large PDFs into
page slices and converts them concurrently server-side (Epic 21), so every
document takes the same single-file submit/resolve path regardless of size.

Chunking is a fully decoupled stage from conversion (Epic 21 §21.8):
/v1/parse/resolve only caches the conversion result (replay JSON + derived
pages) and returns — it no longer submits a chunk job. /v1/chunk/submit,
.../status, and .../finalize own that separately, submitting the
replay-cached DoclingDocument to docling-serve directly from S3.

Both convert and chunk results now come back via an S3Target rather than
inline in the HTTP response (Epic 21 §21.9) — docling-serve writes the
artifact into our own bucket under a per-obj_id scratch prefix, and resolve/
chunk-finalize discover the exact key via a recursive listing rather than
guessing it (docling-jobkit nests the artifact under path segments we don't
fully control). These tests fake the raw Minio client's list_objects() to
exercise that discovery path without a real MinIO.
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
    get_s3_client,
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


def _docling_doc_json() -> bytes:
    return _empty_docling_doc().model_dump_json().encode()


def _mock_docling_client() -> MagicMock:
    """MagicMock for DoclingParseClient with AsyncMock methods."""
    client = MagicMock()
    client.submit_file = AsyncMock(return_value=CONV_TASK_ID)
    client.submit_source = AsyncMock(return_value=CONV_TASK_ID)
    client.get_status = AsyncMock(
        return_value=ParseStatus(status="success", num_processed=1, num_total=1)
    )
    client.submit_chunk = AsyncMock(return_value=CHUNK_TASK_ID)
    client.submit_chunk_source = AsyncMock(return_value=CHUNK_TASK_ID)
    return client


def _metadata(obj_id: uuid.UUID = OBJ_ID) -> str:
    return json.dumps({"obj_id": str(obj_id)})


class _FakeS3Object:
    def __init__(self, object_name: str) -> None:
        self.object_name = object_name


class _FakeS3Client:
    """Stands in for the raw Minio client's list_objects() — the only method
    _discover_s3_result_key uses. Reads/writes go through the InMemoryBlobStore
    `store` fixture instead; this only ever needs to answer "what keys exist
    under this prefix" from that same backing dict."""

    def __init__(self, store: InMemoryBlobStore) -> None:
        self._store = store

    def list_objects(self, bucket: str, prefix: str = "", recursive: bool = False):
        return [
            _FakeS3Object(key) for key in self._store._data if key.startswith(prefix)
        ]


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
    app.dependency_overrides[get_s3_client] = lambda: _FakeS3Client(store)
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
        ref's bucket/key as source AND a per-obj_id scratch prefix as target
        (Epic 21 §21.9 — docling-serve writes the result to S3 rather than
        returning it inline).
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
            target_bucket=settings.REPLAY_CACHE_BUCKET,
            target_key_prefix=parse_service.convert_scratch_prefix(OBJ_ID_STR),
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

    @pytest.fixture(autouse=True)
    def seed_scratch_result(self, store: InMemoryBlobStore) -> str:
        """docling-serve's S3Target write, stood in for directly — mirrors the
        real json/{name}.json nesting under our scratch prefix."""
        prefix = parse_service.convert_scratch_prefix(OBJ_ID_STR)
        key = f"{prefix}json/{OBJ_ID_STR}.json"
        store.put(key, _docling_doc_json())
        return key

    def test_writes_replay_and_pages_cache_without_submitting_chunk(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """
        After resolve: replay cache AND pages cache written, response is just
        the obj_id — chunking is a fully separate stage now (Epic 21 §21.8),
        resolve never submits a chunk job itself.
        """
        resp = client.post("/v1/parse/resolve", json=self._resolve_body())
        assert resp.status_code == 200
        assert resp.json() == {"obj_id": OBJ_ID_STR}
        assert store.exists(f"replay/{OBJ_ID_STR}.json")
        assert store.exists(f"pages/{OBJ_ID_STR}.json")
        docling_client.submit_chunk_source.assert_not_awaited()
        docling_client.submit_chunk.assert_not_awaited()

    def test_scratch_key_deleted_after_resolve(
        self,
        client: TestClient,
        store: InMemoryBlobStore,
        seed_scratch_result: str,
    ) -> None:
        resp = client.post("/v1/parse/resolve", json=self._resolve_body())
        assert resp.status_code == 200
        assert not store.exists(seed_scratch_result)

    def test_replay_cache_matches_discovered_bytes(
        self,
        client: TestClient,
        store: InMemoryBlobStore,
    ) -> None:
        resp = client.post("/v1/parse/resolve", json=self._resolve_body())
        assert resp.status_code == 200
        assert store.get(f"replay/{OBJ_ID_STR}.json") == _docling_doc_json()

    def test_missing_scratch_result_returns_502(
        self,
        client: TestClient,
        store: InMemoryBlobStore,
        seed_scratch_result: str,
    ) -> None:
        """If docling-serve reports success but never wrote the expected
        artifact, resolve must surface a distinct upstream-failure status
        rather than a confusing KeyError-shaped 500."""
        store.delete(seed_scratch_result)
        resp = client.post("/v1/parse/resolve", json=self._resolve_body())
        assert resp.status_code == 502


# ---------------------------------------------------------------------------
# POST /v1/chunk/submit
# ---------------------------------------------------------------------------


class TestChunkSubmitEndpoint:
    def test_submits_replay_cache_from_s3(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """
        /v1/chunk/submit hands docling-serve the replay-cache S3 location as
        source AND a per-obj_id scratch prefix as target — it fetches the
        JSON itself and writes the chunk result back to S3 rather than
        returning it inline (Epic 21 §21.9).
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
            target_bucket=settings.REPLAY_CACHE_BUCKET,
            target_key_prefix=parse_service.chunk_scratch_prefix(OBJ_ID_STR),
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

    @pytest.fixture(autouse=True)
    def seed_scratch_chunks(self, store: InMemoryBlobStore) -> str:
        """docling-serve's S3Target chunk write, stood in for directly —
        newline-delimited JSON, mirroring write_chunks_jsonl's actual format."""
        prefix = parse_service.chunk_scratch_prefix(OBJ_ID_STR)
        key = f"{prefix}chunks/{OBJ_ID_STR}.chunks.jsonl"
        store.put(key, (json.dumps(_CHUNK_ITEM) + "\n").encode())
        return key

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

    def test_scratch_key_deleted_after_finalize(
        self,
        client: TestClient,
        store: InMemoryBlobStore,
        seed_scratch_chunks: str,
    ) -> None:
        resp = client.post("/v1/chunk/finalize", json=self._finalize_body())
        assert resp.status_code == 200
        assert not store.exists(seed_scratch_chunks)

    def test_multiple_chunk_lines_all_parsed(
        self,
        client: TestClient,
        store: InMemoryBlobStore,
        seed_scratch_chunks: str,
    ) -> None:
        second_item = {
            "text": "second chunk",
            "filename": "doc.pdf",
            "page_numbers": [2],
        }
        jsonl = (
            json.dumps(_CHUNK_ITEM) + "\n" + json.dumps(second_item) + "\n"
        ).encode()
        store.put(seed_scratch_chunks, jsonl)
        resp = client.post("/v1/chunk/finalize", json=self._finalize_body())
        assert resp.status_code == 200
        artifact = json.loads(store.get(f"parse/{OBJ_ID_STR}.json"))
        assert len(artifact["chunks"]) == 2

    def test_missing_scratch_result_returns_502(
        self,
        client: TestClient,
        store: InMemoryBlobStore,
        seed_scratch_chunks: str,
    ) -> None:
        store.delete(seed_scratch_chunks)
        resp = client.post("/v1/chunk/finalize", json=self._finalize_body())
        assert resp.status_code == 502
