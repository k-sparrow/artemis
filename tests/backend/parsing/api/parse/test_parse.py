# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the POST /v1/parse endpoint and the _to_parsed_chunk helper.

All infrastructure dependencies (loader factory) are replaced with mocks via
FastAPI's dependency_overrides — no Docling container is required.
"""

from __future__ import annotations

import json
import uuid
from typing import List
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient
from langchain_core.documents import Document

from src.backend.parsing.api.dependencies import get_loader_factory
from src.backend.parsing.api.main import app
from src.backend.parsing.api.parse.service import _to_parsed_chunk
from src.lib.core.adapters.loaders.docling import DoclingConversionError
from src.lib.core.adapters.loaders.exceptions import LoaderError
from src.lib.core.ingestion.exceptions import DocumentProcessingException
from src.lib.core.ingestion.types import ChunkType

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_docs() -> List[Document]:
    return [
        Document(
            page_content="chunk one", metadata={"source": "test.md", "type": "text"}
        ),
        Document(
            page_content="chunk two", metadata={"source": "test.md", "type": "text"}
        ),
    ]


@pytest.fixture
def mock_loader_factory(sample_docs: List[Document]) -> MagicMock:
    loader = MagicMock()
    loader.load = MagicMock(return_value=sample_docs)
    factory = MagicMock(return_value=loader)
    return factory


@pytest.fixture
def client(mock_loader_factory: MagicMock):
    """TestClient with the loader factory replaced by a mock."""
    app.dependency_overrides[get_loader_factory] = lambda: mock_loader_factory
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


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


# ---------------------------------------------------------------------------
# Endpoint tests
# ---------------------------------------------------------------------------


class TestParseEndpoint:
    def test_happy_path_returns_parsed_chunks(
        self, client: TestClient, sample_docs: List[Document]
    ) -> None:
        """Successful parse returns a list of ParsedChunk with correct fields."""
        response = _post_file(client)

        assert response.status_code == 200
        body = response.json()
        assert len(body) == 2
        assert body[0]["page_content"] == "chunk one"
        assert body[0]["source"] == "test.md"
        assert body[0]["type"] == "text"
        assert body[0]["obj_id"] == str(OBJ_ID)
        assert body[1]["page_content"] == "chunk two"

    def test_metadata_stamped_on_all_chunks(self, client: TestClient) -> None:
        """Every key in the metadata dict is stamped onto every returned chunk."""
        custom_id = uuid.uuid4()
        response = _post_file(client, obj_id=custom_id)

        assert response.status_code == 200
        for chunk in response.json():
            assert chunk["obj_id"] == str(custom_id)

    def test_returns_empty_list_for_empty_document(self) -> None:
        """Loader returning no documents → 200 with an empty list."""
        empty_loader = MagicMock()
        empty_loader.load = MagicMock(return_value=[])
        empty_factory = MagicMock(return_value=empty_loader)

        app.dependency_overrides[get_loader_factory] = lambda: empty_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 200
        assert response.json() == []

    def test_docling_connect_error_returns_503(self) -> None:
        """A network-level httpx error (e.g. Docling down) → 503."""
        failing_loader = MagicMock()
        failing_loader.load = MagicMock(
            side_effect=httpx.ConnectError("connection refused")
        )
        failing_factory = MagicMock(return_value=failing_loader)

        app.dependency_overrides[get_loader_factory] = lambda: failing_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 503
        body = response.json()
        assert body["type"] == "upstream_service_error"
        assert body["service"] == "document-loader"
        assert "test.md" in body["detail"]

    def test_loader_error_returns_400(self) -> None:
        """LoaderError from the loader (e.g. unsupported format) → 400."""
        failing_loader = MagicMock()
        failing_loader.load = MagicMock(
            side_effect=DoclingConversionError(
                [
                    {
                        "component_type": "user_input",
                        "module_name": "",
                        "error_message": "File format not allowed: binary.exe",
                    }
                ]
            )
        )
        failing_factory = MagicMock(return_value=failing_loader)

        app.dependency_overrides[get_loader_factory] = lambda: failing_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 400
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "File format not allowed" in body["detail"]

    def test_any_loader_error_subclass_returns_400(self) -> None:
        """Any LoaderError subclass (not just DoclingConversionError) → 400."""

        class _SomeOtherLoaderError(LoaderError):
            pass

        failing_loader = MagicMock()
        failing_loader.load = MagicMock(
            side_effect=_SomeOtherLoaderError("some other loader failed")
        )
        failing_factory = MagicMock(return_value=failing_loader)

        app.dependency_overrides[get_loader_factory] = lambda: failing_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 400
        body = response.json()
        assert body["type"] == "document_processing_error"

    def test_document_processing_exception_returns_400(self) -> None:
        """DocumentProcessingException raised by the loader → 400."""
        failing_loader = MagicMock()
        failing_loader.load = MagicMock(
            side_effect=DocumentProcessingException("unsupported format")
        )
        failing_factory = MagicMock(return_value=failing_loader)

        app.dependency_overrides[get_loader_factory] = lambda: failing_factory
        try:
            with TestClient(app) as c:
                response = _post_file(c)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 400
        body = response.json()
        assert body["type"] == "document_processing_error"
        assert "unsupported format" in body["detail"]

    def test_content_type_forwarded_to_loader_factory(
        self, mock_loader_factory: MagicMock
    ) -> None:
        """The file's MIME type must be passed through to the loader factory unchanged."""
        app.dependency_overrides[get_loader_factory] = lambda: mock_loader_factory
        try:
            with TestClient(app) as c:
                _post_file(
                    c,
                    filename="report.pdf",
                    content=b"%PDF",
                    content_type="application/pdf",
                )
        finally:
            app.dependency_overrides.clear()

        mock_loader_factory.assert_called_once()
        _, _, forwarded_content_type = mock_loader_factory.call_args[0]
        assert forwarded_content_type == "application/pdf"

    def test_unexpected_error_returns_500(self) -> None:
        """Unhandled exceptions from the loader bubble up as 500."""
        crashing_loader = MagicMock()
        crashing_loader.load = MagicMock(side_effect=RuntimeError("unexpected crash"))
        crashing_factory = MagicMock(return_value=crashing_loader)

        app.dependency_overrides[get_loader_factory] = lambda: crashing_factory
        try:
            with TestClient(app, raise_server_exceptions=False) as c:
                response = _post_file(c)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 500


# ---------------------------------------------------------------------------
# _to_parsed_chunk unit tests
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

    def test_missing_source_defaults_to_empty_string(self) -> None:
        doc = Document(
            page_content="text", metadata={"type": "text", "obj_id": str(OBJ_ID)}
        )
        chunk = _to_parsed_chunk(doc)
        assert chunk.source == ""

    def test_missing_type_defaults_to_text(self) -> None:
        doc = Document(
            page_content="text", metadata={"source": "a.md", "obj_id": str(OBJ_ID)}
        )
        chunk = _to_parsed_chunk(doc)
        assert chunk.type == ChunkType.TEXT

    def test_table_type_preserved(self) -> None:
        doc = Document(
            page_content="| col1 | col2 |",
            metadata={"source": "report.pdf", "type": "table", "obj_id": str(OBJ_ID)},
        )
        chunk = _to_parsed_chunk(doc)
        assert chunk.type == ChunkType.TABLE

    def test_unknown_type_falls_back_to_unknown(self) -> None:
        """A DocItemLabel value not yet in ChunkType maps to UNKNOWN, not a crash."""
        doc = Document(
            page_content="some content",
            metadata={
                "source": "doc.pdf",
                "type": "future_label_we_dont_know",
                "obj_id": str(OBJ_ID),
            },
        )
        chunk = _to_parsed_chunk(doc)
        assert chunk.type == ChunkType.UNKNOWN

    def test_source_and_obj_id_are_distinct(self) -> None:
        """
        source (filename/path) and obj_id must be separate, non-equal fields
        on ParsedChunk.
        """
        doc = Document(
            page_content="content",
            metadata={"source": "report.pdf", "type": "text", "obj_id": str(OBJ_ID)},
        )
        chunk = _to_parsed_chunk(doc)

        assert chunk.source == "report.pdf"
        assert chunk.obj_id == OBJ_ID
        assert chunk.source != str(chunk.obj_id)

    def test_all_extended_chunk_types_are_valid(self) -> None:
        """Every ChunkType value (except UNKNOWN) round-trips through _to_parsed_chunk."""
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
            chunk = _to_parsed_chunk(doc)
            assert chunk.type == chunk_type
