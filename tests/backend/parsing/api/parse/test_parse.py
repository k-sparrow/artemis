# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the POST /v1/parse endpoint and the _to_parsed_chunk helper.

The endpoint is claim-check: it resolves input bytes (inline ``file`` or a
``source_ref`` BlobRef), parses, writes the artifact to object storage, and
returns the artifact's ``BlobRef``. Both the loader factory and the blob-store
factory are replaced via FastAPI's dependency_overrides — a single in-memory
``BlobStore`` backs both the source-read and artifact-write paths, so no MinIO
(and no Docling container) is required.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from typing import List
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from langchain_core.documents import Document

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import (
    get_blob_store_factory,
    get_loader_factory,
)
from src.backend.parsing.api.main import app
from src.backend.parsing.api.parse.service import _to_parsed_chunk
from src.lib.core.adapters.loaders.docling import DoclingConversionError
from src.lib.core.adapters.loaders.exceptions import LoaderError
from src.lib.core.adapters.stores.memory.blob import InMemoryBlobStore
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.types import ChunkType

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")
ARTIFACT_BUCKET = settings.PARSED_ARTIFACTS_BUCKET
REPLAY_BUCKET = settings.REPLAY_CACHE_BUCKET

# Sample page parents the fake loader returns alongside the chunks.
SAMPLE_PAGES = [(1, "# Hello\n\nWorld")]
# Stand-in for the lossless DoclingDocument JSON written to the replay cache.
REPLAY_JSON = '{"schema_name":"DoclingDocument"}'


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _fake_dl_doc() -> MagicMock:
    """Opaque stand-in for a DoclingDocument; only ``model_dump_json`` is used."""
    doc = MagicMock()
    doc.model_dump_json = MagicMock(return_value=REPLAY_JSON)
    return doc


def _loader_returning(
    docs: List[Document], pages: list[tuple[int, str]] | None = None
) -> MagicMock:
    """Loader whose ``aload_artifact`` yields the given chunk docs + pages."""
    loader = MagicMock()
    loader.aload_artifact = AsyncMock(
        return_value=(
            docs,
            pages if pages is not None else SAMPLE_PAGES,
            _fake_dl_doc(),
        )
    )
    return loader


def _loader_raising(exc: Exception) -> MagicMock:
    loader = MagicMock()
    loader.aload_artifact = AsyncMock(side_effect=exc)
    return loader


@pytest.fixture
def sample_docs() -> List[Document]:
    return [
        Document(
            page_content="chunk one",
            metadata={"source": "test.md", "type": "text", "page_no": 1},
        ),
        Document(
            page_content="chunk two",
            metadata={"source": "test.md", "type": "text", "page_no": 1},
        ),
    ]


@pytest.fixture
def mock_loader_factory(sample_docs: List[Document]) -> MagicMock:
    return MagicMock(return_value=_loader_returning(sample_docs))


@pytest.fixture
def store() -> InMemoryBlobStore:
    """One in-memory store backing both source-read and artifact-write."""
    return InMemoryBlobStore()


@pytest.fixture
def make_client(store: InMemoryBlobStore):
    """Return a context manager yielding a TestClient with the given loader
    factory and the blob-store factory stubbed by the shared in-memory store."""

    @contextmanager
    def _make(loader_factory, *, raise_server_exceptions: bool = True):
        app.dependency_overrides[get_loader_factory] = lambda: loader_factory
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
def client(make_client, mock_loader_factory: MagicMock):
    with make_client(mock_loader_factory) as c:
        yield c


def _artifact(store: InMemoryBlobStore, ref: dict) -> dict:
    """Read the ParseArtifact the endpoint wrote (pages + chunks)."""
    return json.loads(store.get(ref["key"]))


def _artifact_chunks(store: InMemoryBlobStore, ref: dict) -> list[dict]:
    """Read the artifact the endpoint wrote and return its chunk dicts."""
    return _artifact(store, ref)["chunks"]


def _post_file(
    client: TestClient,
    filename: str = "test.md",
    content: bytes = b"# Hello\n\nWorld",
    content_type: str = "text/markdown",
    obj_id: uuid.UUID = OBJ_ID,
    extra_metadata: dict[str, str] | None = None,
):
    metadata = {"obj_id": str(obj_id), **(extra_metadata or {})}
    return client.post(
        "/v1/parse",
        files={"file": (filename, content, content_type)},
        data={"metadata": json.dumps(metadata)},
    )


def _post_ref(
    client: TestClient,
    bucket: str,
    key: str,
    filename: str = "test.md",
    content_type: str = "text/markdown",
    obj_id: uuid.UUID = OBJ_ID,
):
    return client.post(
        "/v1/parse",
        data={
            "source_ref": json.dumps({"bucket": bucket, "key": key}),
            "filename": filename,
            "content_type": content_type,
            "metadata": json.dumps({"obj_id": str(obj_id)}),
        },
    )


# ---------------------------------------------------------------------------
# Endpoint tests — happy paths
# ---------------------------------------------------------------------------


class TestParseEndpoint:
    def test_inline_writes_artifact_and_returns_ref(
        self, client: TestClient, store: InMemoryBlobStore
    ) -> None:
        """Inline upload → 200, returns the artifact BlobRef, chunks written to it."""
        response = _post_file(client)

        assert response.status_code == 200
        ref = response.json()
        assert ref == {"bucket": ARTIFACT_BUCKET, "key": f"parse/{OBJ_ID}.json"}

        artifact = _artifact(store, ref)
        chunks = artifact["chunks"]
        assert len(chunks) == 2
        assert chunks[0]["page_content"] == "chunk one"
        assert chunks[0]["source"] == "test.md"
        assert chunks[0]["type"] == "text"
        assert chunks[0]["obj_id"] == str(OBJ_ID)
        assert chunks[0]["page_no"] == 1
        assert chunks[1]["page_content"] == "chunk two"

        # Page parents travel in the same artifact; dl_meta never does.
        assert artifact["pages"] == [{"page_no": 1, "markdown": "# Hello\n\nWorld"}]
        assert "dl_meta" not in chunks[0]

        # The lossless replay cache is written to its private bucket.
        assert store.get(f"replay/{OBJ_ID}.json").decode() == REPLAY_JSON

    def test_replay_cache_written_to_private_bucket(
        self, client: TestClient, store: InMemoryBlobStore
    ) -> None:
        """Replay cache is keyed by obj_id and holds the DoclingDocument JSON.

        The in-memory store backs every bucket, so we assert on the key; the
        bucket name (``REPLAY_BUCKET``) is exercised by the integration tier.
        """
        _post_file(client)
        assert REPLAY_BUCKET  # configured, distinct from the artifact bucket
        assert store.exists(f"replay/{OBJ_ID}.json")

    def test_source_ref_reads_input_then_writes_artifact(
        self, client: TestClient, store: InMemoryBlobStore
    ) -> None:
        """source_ref path: input is read from the store by key, artifact written back."""
        store.put("artemis/in.md", b"# Hello\n\nWorld")
        response = _post_ref(client, bucket="artemis", key="artemis/in.md")

        assert response.status_code == 200
        ref = response.json()
        assert ref == {"bucket": ARTIFACT_BUCKET, "key": f"parse/{OBJ_ID}.json"}
        assert len(_artifact_chunks(store, ref)) == 2

    def test_metadata_obj_id_stamped_on_all_chunks(
        self, client: TestClient, store: InMemoryBlobStore
    ) -> None:
        custom_id = uuid.uuid4()
        response = _post_file(client, obj_id=custom_id)

        assert response.status_code == 200
        for chunk in _artifact_chunks(store, response.json()):
            assert chunk["obj_id"] == str(custom_id)

    def test_empty_document_writes_empty_artifact(
        self, make_client, store: InMemoryBlobStore
    ) -> None:
        empty_loader = _loader_returning([], pages=[])
        with make_client(MagicMock(return_value=empty_loader)) as c:
            response = _post_file(c)

        assert response.status_code == 200
        artifact = _artifact(store, response.json())
        assert artifact["chunks"] == []
        assert artifact["pages"] == []

    def test_requires_exactly_one_input(self, client: TestClient) -> None:
        """Neither file nor source_ref → 422."""
        response = client.post(
            "/v1/parse", data={"metadata": json.dumps({"obj_id": str(OBJ_ID)})}
        )
        assert response.status_code == 422

    def test_content_type_forwarded_to_loader_factory(
        self, make_client, mock_loader_factory: MagicMock
    ) -> None:
        with make_client(mock_loader_factory) as c:
            _post_file(
                c,
                filename="report.pdf",
                content=b"%PDF",
                content_type="application/pdf",
            )

        mock_loader_factory.assert_called_once()
        _, _, forwarded_content_type = mock_loader_factory.call_args[0]
        assert forwarded_content_type == "application/pdf"

    # -- error mapping (unchanged behaviour; happens before the artifact write) --

    def test_docling_connect_error_returns_503(self, make_client) -> None:
        failing = _loader_raising(httpx.ConnectError("connection refused"))
        with make_client(MagicMock(return_value=failing)) as c:
            response = _post_file(c)

        assert response.status_code == 503
        body = response.json()
        assert body["type"] == "upstream_service_error"
        assert body["service"] == "document-loader"
        assert "test.md" in body["detail"]

    def test_loader_error_returns_400(self, make_client) -> None:
        failing = _loader_raising(
            DoclingConversionError(
                [
                    {
                        "component_type": "user_input",
                        "module_name": "",
                        "error_message": "File format not allowed: binary.exe",
                    }
                ]
            )
        )
        with make_client(MagicMock(return_value=failing)) as c:
            response = _post_file(c)

        assert response.status_code == 400
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "File format not allowed" in body["detail"]

    def test_any_loader_error_subclass_returns_400(self, make_client) -> None:
        class _SomeOtherLoaderError(LoaderError):
            pass

        failing = _loader_raising(_SomeOtherLoaderError("some other"))
        with make_client(MagicMock(return_value=failing)) as c:
            response = _post_file(c)

        assert response.status_code == 400
        assert response.json()["type"] == "document_processing_error"

    def test_document_processing_exception_returns_400(self, make_client) -> None:
        failing = _loader_raising(DocumentProcessingException("unsupported format"))
        with make_client(MagicMock(return_value=failing)) as c:
            response = _post_file(c)

        assert response.status_code == 400
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "unsupported format" in body["detail"]

    def test_unexpected_error_returns_500(self, make_client) -> None:
        crashing = _loader_raising(RuntimeError("unexpected crash"))
        with make_client(
            MagicMock(return_value=crashing), raise_server_exceptions=False
        ) as c:
            response = _post_file(c)

        assert response.status_code == 500


# ---------------------------------------------------------------------------
# _to_parsed_chunk unit tests (unchanged)
# ---------------------------------------------------------------------------


class TestToParsedChunk:
    def test_all_fields_preserved(self) -> None:
        doc = Document(
            page_content="some text",
            metadata={"source": "doc.pdf", "type": "text", "obj_id": str(OBJ_ID)},
        )
        chunk = _to_parsed_chunk(doc)

        assert chunk.page_content == "some text"
        assert chunk.source == "doc.pdf"
        assert chunk.type == ChunkType.TEXT
        assert chunk.obj_id == OBJ_ID

    def test_page_no_carried_when_present(self) -> None:
        doc = Document(
            page_content="text",
            metadata={
                "source": "a.md",
                "type": "text",
                "obj_id": str(OBJ_ID),
                "page_no": 4,
            },
        )
        assert _to_parsed_chunk(doc).page_no == 4

    def test_page_no_defaults_to_none(self) -> None:
        doc = Document(
            page_content="text",
            metadata={"source": "a.md", "type": "text", "obj_id": str(OBJ_ID)},
        )
        assert _to_parsed_chunk(doc).page_no is None

    def test_missing_source_defaults_to_empty_string(self) -> None:
        doc = Document(
            page_content="text", metadata={"type": "text", "obj_id": str(OBJ_ID)}
        )
        assert _to_parsed_chunk(doc).source == ""

    def test_missing_type_defaults_to_text(self) -> None:
        doc = Document(
            page_content="text", metadata={"source": "a.md", "obj_id": str(OBJ_ID)}
        )
        assert _to_parsed_chunk(doc).type == ChunkType.TEXT

    def test_table_type_preserved(self) -> None:
        doc = Document(
            page_content="| col1 | col2 |",
            metadata={"source": "report.pdf", "type": "table", "obj_id": str(OBJ_ID)},
        )
        assert _to_parsed_chunk(doc).type == ChunkType.TABLE

    def test_unknown_type_falls_back_to_unknown(self) -> None:
        doc = Document(
            page_content="some content",
            metadata={
                "source": "doc.pdf",
                "type": "future_label_we_dont_know",
                "obj_id": str(OBJ_ID),
            },
        )
        assert _to_parsed_chunk(doc).type == ChunkType.UNKNOWN

    def test_source_and_obj_id_are_distinct(self) -> None:
        doc = Document(
            page_content="content",
            metadata={"source": "report.pdf", "type": "text", "obj_id": str(OBJ_ID)},
        )
        chunk = _to_parsed_chunk(doc)
        assert chunk.source == "report.pdf"
        assert chunk.obj_id == OBJ_ID
        assert chunk.source != str(chunk.obj_id)

    def test_all_extended_chunk_types_are_valid(self) -> None:
        for chunk_type in ChunkType:
            if chunk_type is ChunkType.UNKNOWN:
                continue
            doc = Document(
                page_content="content",
                metadata={
                    "source": "doc.pdf",
                    "type": chunk_type.value,
                    "obj_id": str(OBJ_ID),
                },
            )
            assert _to_parsed_chunk(doc).type == chunk_type
