"""
Unit tests for POST /v1/parse/submit|status|resolve and
/v1/chunk/submit|status|finalize.

Covers the async-path endpoints (Epic 18). Large-PDF handling is no longer a
client-side concern here — docling-serve's Ray engine fans large PDFs into
page slices and converts them concurrently server-side (Epic 21), so every
document takes the same single-file submit/resolve path regardless of size.

Chunking is a fully decoupled stage from conversion (Epic 21 §21.8) —
/v1/parse/resolve only caches the conversion result (replay JSON + derived
pages) and returns — it no longer submits a chunk job. /v1/chunk/submit,
.../status, and .../finalize own that separately, and always stay inline
(multipart bytes for the source, JSON body for chunk); no chunk endpoint in
docling-serve v1.29.0 accepts an S3 source, batch or not.

Convert submission (/v1/parse/submit), however, branches on input: ``file``
stays inline; ``source_ref`` is dispatched S3-direct — this service only
verifies the referenced object exists (never reads it) and passes the
bucket/key straight through docling-serve's `/v1/convert/source/batch`
(requires the Ray-serde patch, see tools/oci/images/docling). That endpoint's
target has no inbody option, so the result comes back via S3Target rather
than inline; /v1/parse/resolve discovers the written key via a recursive
listing. `SubmitResult.mode` ("file"/"source") threads through the worker's
resolve call unmodified so resolve knows which retrieval path to use — these
tests fake the raw Minio client's list_objects() to exercise that discovery
without a real MinIO.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import httpx
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
    client.fetch_conversion_result = AsyncMock(return_value=_empty_docling_doc())
    client.submit_chunk = AsyncMock(return_value=CHUNK_TASK_ID)
    client.fetch_chunk_result = AsyncMock(return_value=[_CHUNK_ITEM])
    return client


def _metadata(obj_id: uuid.UUID = OBJ_ID) -> str:
    return json.dumps({"obj_id": str(obj_id)})


def _docling_status_error(status_code: int) -> httpx.HTTPStatusError:
    """An httpx error as raised by DoclingParseClient's raise_for_status()."""
    request = httpx.Request("POST", "http://test-docling:5001/v1/convert/file/async")
    response = httpx.Response(status_code, request=request, text="boom")
    return httpx.HTTPStatusError(
        f"{status_code} error", request=request, response=response
    )


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

    def test_source_ref_path_dispatches_s3_direct(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """
        source_ref form field → this service verifies the object exists
        (never reads it) and calls docling_client.submit_source with the
        bucket/key untouched plus a per-obj_id scratch target — submit_file
        is never called on this path.
        """
        store.put("ingest-source/doc.md", b"# Hello from source_ref")
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
        body = resp.json()
        assert body["parsing_task_id"] == CONV_TASK_ID
        assert body["mode"] == "source"
        docling_client.submit_source.assert_awaited_once_with(
            s3_endpoint=settings.S3_ENDPOINT,
            s3_access_key=settings.S3_ACCESS_KEY,
            s3_secret_key=settings.S3_SECRET_KEY,
            s3_secure=settings.S3_SECURE,
            bucket="ingest-source",
            key="ingest-source/doc.md",
            target_bucket=settings.REPLAY_CACHE_BUCKET,
            target_key_prefix=parse_service.convert_scratch_prefix(OBJ_ID_STR),
            callbacks=[
                {
                    "url": f"{settings.PARSING_SERVICE_PUBLIC_URL}"
                    f"/v1/parse/callback/{OBJ_ID_STR}"
                }
            ],
            timeout=120.0,
        )
        docling_client.submit_file.assert_not_awaited()

    def test_source_ref_nonexistent_returns_422(
        self,
        client: TestClient,
        docling_client: MagicMock,
    ) -> None:
        """A stale/invalid source_ref fails fast with 422 rather than
        dispatching a job to docling-serve that would fail asynchronously."""
        resp = client.post(
            "/v1/parse/submit",
            data={
                "source_ref": json.dumps(
                    {"bucket": "ingest-source", "key": "does-not-exist.pdf"}
                ),
                "metadata": _metadata(),
            },
        )
        assert resp.status_code == 422
        docling_client.submit_source.assert_not_awaited()
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
    def _resolve_body(self, mode: str = "file") -> dict:
        return {"parsing_task_id": CONV_TASK_ID, "obj_id": OBJ_ID_STR, "mode": mode}

    def test_fetches_conversion_result(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        resp = client.post("/v1/parse/resolve", json=self._resolve_body(mode="file"))
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
        resp = client.post("/v1/parse/resolve", json=self._resolve_body(mode="file"))
        assert resp.status_code == 200
        assert resp.json() == {"obj_id": OBJ_ID_STR}
        assert store.exists(f"replay/{OBJ_ID_STR}.json")
        assert store.exists(f"pages/{OBJ_ID_STR}.json")
        docling_client.submit_chunk.assert_not_awaited()

    def test_source_mode_discovers_result_from_s3_scratch_prefix(
        self,
        client: TestClient,
        docling_client: MagicMock,
        store: InMemoryBlobStore,
    ) -> None:
        """
        mode="source" (the S3-direct convert path) never calls
        fetch_conversion_result — docling-serve wrote the result to the
        S3Target scratch prefix instead (batch endpoint has no inbody
        target). Resolve discovers it via listing, copies it to the stable
        replay key, and deletes the scratch object.
        """
        scratch_prefix = parse_service.convert_scratch_prefix(OBJ_ID_STR)
        scratch_key = f"{scratch_prefix}json/doc.json"
        store.put(scratch_key, _docling_doc_json())

        resp = client.post("/v1/parse/resolve", json=self._resolve_body(mode="source"))

        assert resp.status_code == 200
        assert resp.json() == {"obj_id": OBJ_ID_STR}
        assert store.get(f"replay/{OBJ_ID_STR}.json") == _docling_doc_json()
        assert not store.exists(scratch_key)
        assert store.exists(f"pages/{OBJ_ID_STR}.json")
        docling_client.fetch_conversion_result.assert_not_awaited()

    def test_source_mode_missing_result_returns_502(self, client: TestClient) -> None:
        """No object under the scratch prefix — docling-serve claimed success
        but wrote nothing — surfaces as a clear 502, not a silent empty artifact."""
        resp = client.post("/v1/parse/resolve", json=self._resolve_body(mode="source"))
        assert resp.status_code == 502


# ---------------------------------------------------------------------------
# POST /v1/chunk/submit
# ---------------------------------------------------------------------------


class TestChunkSubmitEndpoint:
    @pytest.fixture(autouse=True)
    def seed_replay_cache(self, store: InMemoryBlobStore) -> None:
        store.put(f"replay/{OBJ_ID_STR}.json", _docling_doc_json())

    def test_submits_replay_cache_inline(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """
        /v1/chunk/submit reads the replay-cached DoclingDocument itself and
        submits it inline — no docling-serve v1.29.0 endpoint can actually
        take an S3 source (see module docstring).
        """
        resp = client.post("/v1/chunk/submit", json={"obj_id": OBJ_ID_STR})
        assert resp.status_code == 200
        assert resp.json() == {
            "chunking_task_id": CHUNK_TASK_ID,
            "obj_id": OBJ_ID_STR,
        }
        docling_client.submit_chunk.assert_awaited_once_with(
            _docling_doc_json(), timeout=120.0
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
        pages = [
            Page(obj_id=OBJ_ID, page_no=1, markdown="# Seeded Page", source="doc.pdf")
        ]
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


# ---------------------------------------------------------------------------
# docling-serve failure mapping (_translate_docling_errors)
# ---------------------------------------------------------------------------
#
# The worker's retry logic (controller/worker/tasks.py) branches on
# ``status_code >= 500`` to decide retry-vs-permanent. These tests pin the
# contract from this service's side: docling-serve unreachable or erroring
# 5xx must surface as 503 (retryable); a docling-serve 4xx must surface as
# 400 (permanent) rather than a raw exception turning into a generic 500 that
# the worker can't classify.


class TestSubmitEndpointDoclingErrorMapping:
    """Exercised in full here; the sibling endpoints below only need one
    representative case each to confirm the decorator is actually applied —
    the mapping logic itself is shared and already covered exhaustively."""

    def _post_file(self, client: TestClient) -> httpx.Response:
        return client.post(
            "/v1/parse/submit",
            files={"file": ("report.md", b"# Hello", "text/markdown")},
            data={"metadata": _metadata()},
        )

    def test_connect_error_returns_503(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.submit_file.side_effect = httpx.ConnectError(
            "connection refused"
        )
        resp = self._post_file(client)
        assert resp.status_code == 503
        body = resp.json()
        assert body["type"] == "upstream_service_error"
        assert body["service"] == "docling-serve"

    def test_read_timeout_returns_503(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.submit_file.side_effect = httpx.ReadTimeout("timed out")
        resp = self._post_file(client)
        assert resp.status_code == 503
        assert resp.json()["type"] == "upstream_service_error"

    def test_docling_serve_500_returns_503(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.submit_file.side_effect = _docling_status_error(500)
        resp = self._post_file(client)
        assert resp.status_code == 503
        assert resp.json()["type"] == "upstream_service_error"

    def test_docling_serve_400_returns_400(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.submit_file.side_effect = _docling_status_error(400)
        resp = self._post_file(client)
        assert resp.status_code == 400
        body = resp.json()
        assert body["type"] == "document_processing_error"
        assert "boom" in body["detail"]

    def test_docling_serve_422_returns_400(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        """Any docling-serve 4xx (not just 400) is a permanent, non-retryable
        failure from the worker's perspective — all map to 400 here."""
        docling_client.submit_file.side_effect = _docling_status_error(422)
        resp = self._post_file(client)
        assert resp.status_code == 400
        assert resp.json()["type"] == "document_processing_error"


class TestStatusEndpointDoclingErrorMapping:
    def test_connect_error_returns_503(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.get_status.side_effect = httpx.ConnectError("refused")
        resp = client.get(f"/v1/parse/status/{CONV_TASK_ID}")
        assert resp.status_code == 503
        assert resp.json()["type"] == "upstream_service_error"


class TestResolveEndpointDoclingErrorMapping:
    def test_connect_error_returns_503(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.fetch_conversion_result.side_effect = httpx.ConnectError(
            "refused"
        )
        resp = client.post(
            "/v1/parse/resolve",
            json={
                "parsing_task_id": CONV_TASK_ID,
                "obj_id": OBJ_ID_STR,
                "mode": "file",
            },
        )
        assert resp.status_code == 503
        assert resp.json()["type"] == "upstream_service_error"


class TestChunkSubmitEndpointDoclingErrorMapping:
    def test_docling_serve_400_returns_400(
        self, client: TestClient, docling_client: MagicMock, store: InMemoryBlobStore
    ) -> None:
        store.put(f"replay/{OBJ_ID_STR}.json", _docling_doc_json())
        docling_client.submit_chunk.side_effect = _docling_status_error(400)
        resp = client.post("/v1/chunk/submit", json={"obj_id": OBJ_ID_STR})
        assert resp.status_code == 400
        assert resp.json()["type"] == "document_processing_error"


class TestChunkStatusEndpointDoclingErrorMapping:
    def test_connect_error_returns_503(
        self, client: TestClient, docling_client: MagicMock
    ) -> None:
        docling_client.get_status.side_effect = httpx.ConnectError("refused")
        resp = client.get(f"/v1/chunk/status/{CHUNK_TASK_ID}")
        assert resp.status_code == 503
        assert resp.json()["type"] == "upstream_service_error"


class TestChunkFinalizeEndpointDoclingErrorMapping:
    def test_connect_error_returns_503(
        self, client: TestClient, docling_client: MagicMock, store: InMemoryBlobStore
    ) -> None:
        pages = [
            Page(obj_id=OBJ_ID, page_no=1, markdown="# Seeded Page", source="doc.pdf")
        ]
        store.put(f"pages/{OBJ_ID_STR}.json", parse_service.encode_pages(pages))
        docling_client.fetch_chunk_result.side_effect = httpx.ConnectError("refused")
        resp = client.post(
            "/v1/chunk/finalize",
            json={
                "chunking_task_id": CHUNK_TASK_ID,
                "obj_id": OBJ_ID_STR,
                "metadata": {"obj_id": OBJ_ID_STR},
            },
        )
        assert resp.status_code == 503
        assert resp.json()["type"] == "upstream_service_error"
