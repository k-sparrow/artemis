# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for DoclingAPIServeLoader and MetaExtractor.

All tests are pure unit tests — no Docling container, no HTTP calls.
The converter is mocked so tests run in CI without any infrastructure.

TestMetaExtractor
    Pure label-classification logic: TABLE priority, first-label fallback,
    empty-labels default, source propagation.

TestDoclingAPIServeLoaderUnit
    Loader dispatch logic with a mocked converter.  Verifies ExportType
    routing, Document shape, metadata, error propagation, and async paths.

The integration tests (real Docling container) live in
``test_docling_loader_integration.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from docling_core.types.doc.labels import DocItemLabel
from langchain_core.documents import Document

from src.lib.core.adapters.loaders.docling import (
    DoclingAPIServeLoader,
    DoclingConversionError,
    DoclingServeAPIDocumentConverter,
    ExportType,
    MetaExtractor,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_MARKDOWN_FILE: tuple[str, bytes, str] = (
    "test.md",
    b"# Hello\n\nThis is a test document with some content.\n",
    "text/markdown",
)


def _make_chunk(labels: list[DocItemLabel]) -> MagicMock:
    """Build a minimal fake BaseChunk with the given DocItemLabel list."""
    chunk = MagicMock()
    chunk.meta.doc_items = [MagicMock(label=label) for label in labels]
    chunk.meta.export_json_dict.return_value = {}
    return chunk


def _make_dl_doc(markdown: str = "# Doc\n\nContent.") -> MagicMock:
    """Build a minimal fake DoclingDocument."""
    dl_doc = MagicMock()
    dl_doc.export_to_markdown.return_value = markdown
    return dl_doc


def _make_loader(
    export_type: ExportType = ExportType.DOC_CHUNKS,
    chunks: list[MagicMock] | None = None,
    markdown: str = "# Doc\n\nContent.",
) -> tuple[DoclingAPIServeLoader, MagicMock, MagicMock]:
    """Return (loader, mock_converter, mock_chunker)."""
    dl_doc = _make_dl_doc(markdown)

    mock_converter = MagicMock()
    mock_converter.convert.return_value = dl_doc
    mock_converter.aconvert = AsyncMock(return_value=dl_doc)

    mock_chunker = MagicMock()
    mock_chunker.chunk.return_value = iter(chunks or [])
    mock_chunker.contextualize.return_value = "chunk text"

    loader = DoclingAPIServeLoader(
        source=_MARKDOWN_FILE,
        converter=mock_converter,
        chunker=mock_chunker,
        export_type=export_type,
    )
    return loader, mock_converter, mock_chunker


# ---------------------------------------------------------------------------
# MetaExtractor
# ---------------------------------------------------------------------------


class TestMetaExtractor:
    """Label-classification logic — no network, no container."""

    def test_table_label_takes_priority_over_text(self) -> None:
        """When a chunk has both TEXT and TABLE labels, TABLE wins."""
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.TEXT, DocItemLabel.TABLE])
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["type"] == DocItemLabel.TABLE.value

    def test_table_label_takes_priority_regardless_of_position(self) -> None:
        """TABLE priority applies even when it is the last label."""
        extractor = MetaExtractor()
        chunk = _make_chunk(
            [DocItemLabel.PARAGRAPH, DocItemLabel.TEXT, DocItemLabel.TABLE]
        )
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["type"] == DocItemLabel.TABLE.value

    def test_first_label_used_when_no_table(self) -> None:
        """Without a TABLE label, the first item's label is used."""
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.PARAGRAPH, DocItemLabel.TEXT])
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["type"] == DocItemLabel.PARAGRAPH.value

    def test_empty_labels_defaults_to_text(self) -> None:
        """A chunk with no doc_items falls back to TEXT."""
        extractor = MetaExtractor()
        chunk = _make_chunk([])
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["type"] == DocItemLabel.TEXT.value

    def test_source_field_equals_file_path(self) -> None:
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.TEXT])
        meta = extractor.extract_chunk_meta("path/to/doc.pdf", chunk)
        assert meta["source"] == "path/to/doc.pdf"

    def test_dl_meta_included_from_export_json_dict(self) -> None:
        """dl_meta key is populated from chunk.meta.export_json_dict()."""
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.TEXT])
        chunk.meta.export_json_dict.return_value = {"heading": "Intro"}
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["dl_meta"] == {"heading": "Intro"}

    def test_extract_dl_doc_meta_returns_source(self) -> None:
        extractor = MetaExtractor()
        meta = extractor.extract_dl_doc_meta("report.pdf", MagicMock())
        assert meta == {"source": "report.pdf"}


# ---------------------------------------------------------------------------
# DoclingAPIServeLoader — ExportType.MARKDOWN
# ---------------------------------------------------------------------------


class TestDoclingAPIServeLoaderMarkdown:
    """Loader in MARKDOWN mode — converter is mocked."""

    def test_yields_exactly_one_document(self) -> None:
        loader, _, _ = _make_loader(export_type=ExportType.MARKDOWN)
        docs = list(loader.lazy_load())
        assert len(docs) == 1
        assert isinstance(docs[0], Document)

    def test_page_content_is_markdown_string(self) -> None:
        loader, _, _ = _make_loader(
            export_type=ExportType.MARKDOWN, markdown="# Title\n\nBody."
        )
        docs = list(loader.lazy_load())
        assert docs[0].page_content == "# Title\n\nBody."

    def test_source_metadata(self) -> None:
        loader, _, _ = _make_loader(export_type=ExportType.MARKDOWN)
        docs = list(loader.lazy_load())
        assert docs[0].metadata["source"] == "test.md"

    def test_convert_called_once(self) -> None:
        loader, mock_converter, _ = _make_loader(export_type=ExportType.MARKDOWN)
        list(loader.lazy_load())
        mock_converter.convert.assert_called_once()

    @pytest.mark.asyncio
    async def test_alazy_load_yields_one_document(self) -> None:
        loader, mock_converter, _ = _make_loader(export_type=ExportType.MARKDOWN)
        docs = [doc async for doc in loader.alazy_load()]
        assert len(docs) == 1
        mock_converter.aconvert.assert_called_once()


# ---------------------------------------------------------------------------
# DoclingAPIServeLoader — ExportType.DOC_CHUNKS
# ---------------------------------------------------------------------------


class TestDoclingAPIServeLoaderDocChunks:
    """Loader in DOC_CHUNKS mode — converter and chunker are mocked."""

    def test_yields_one_doc_per_chunk(self) -> None:
        fake_chunks = [_make_chunk([DocItemLabel.TEXT]) for _ in range(3)]
        loader, _, _ = _make_loader(
            export_type=ExportType.DOC_CHUNKS, chunks=fake_chunks
        )
        docs = list(loader.lazy_load())
        assert len(docs) == 3

    def test_source_metadata_on_every_doc(self) -> None:
        chunk = _make_chunk([DocItemLabel.TEXT])
        loader, _, _ = _make_loader(export_type=ExportType.DOC_CHUNKS, chunks=[chunk])
        docs = list(loader.lazy_load())
        assert docs[0].metadata["source"] == "test.md"

    def test_empty_document_yields_no_docs(self) -> None:
        loader, _, _ = _make_loader(export_type=ExportType.DOC_CHUNKS, chunks=[])
        assert list(loader.lazy_load()) == []

    @pytest.mark.asyncio
    async def test_alazy_load_yields_per_chunk(self) -> None:
        fake_chunks = [_make_chunk([DocItemLabel.TEXT]) for _ in range(2)]
        loader, _, _ = _make_loader(
            export_type=ExportType.DOC_CHUNKS, chunks=fake_chunks
        )
        docs = [doc async for doc in loader.alazy_load()]
        assert len(docs) == 2


# ---------------------------------------------------------------------------
# DoclingAPIServeLoader — error handling
# ---------------------------------------------------------------------------


class TestDoclingAPIServeLoaderErrors:
    def test_http_error_propagates_from_lazy_load(self) -> None:
        loader, mock_converter, _ = _make_loader(export_type=ExportType.MARKDOWN)
        mock_converter.convert.side_effect = httpx.ConnectError("refused")
        with pytest.raises(httpx.ConnectError):
            list(loader.lazy_load())

    @pytest.mark.asyncio
    async def test_http_error_propagates_from_alazy_load(self) -> None:
        loader, mock_converter, _ = _make_loader(export_type=ExportType.MARKDOWN)
        mock_converter.aconvert.side_effect = httpx.ConnectError("refused")
        with pytest.raises(httpx.ConnectError):
            _ = [doc async for doc in loader.alazy_load()]

    def test_conversion_error_propagates_from_lazy_load(self) -> None:
        """DoclingConversionError raised by converter surfaces from lazy_load."""
        loader, mock_converter, _ = _make_loader(export_type=ExportType.MARKDOWN)
        mock_converter.convert.side_effect = DoclingConversionError(
            [
                {
                    "component_type": "user_input",
                    "module_name": "",
                    "error_message": "File format not allowed: binary.exe",
                }
            ]
        )
        with pytest.raises(DoclingConversionError, match="File format not allowed"):
            list(loader.lazy_load())

    @pytest.mark.asyncio
    async def test_conversion_error_propagates_from_alazy_load(self) -> None:
        """DoclingConversionError raised by converter surfaces from alazy_load."""
        loader, mock_converter, _ = _make_loader(export_type=ExportType.MARKDOWN)
        mock_converter.aconvert.side_effect = DoclingConversionError(
            [
                {
                    "component_type": "user_input",
                    "module_name": "",
                    "error_message": "File format not allowed: binary.exe",
                }
            ]
        )
        with pytest.raises(DoclingConversionError, match="File format not allowed"):
            _ = [doc async for doc in loader.alazy_load()]

    def test_unknown_export_type_raises_value_error(self) -> None:
        loader, _, _ = _make_loader()
        loader._export_type = "unsupported"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="Unexpected export type"):
            list(loader.lazy_load())

    @pytest.mark.asyncio
    async def test_unknown_export_type_raises_on_alazy_load(self) -> None:
        loader, _, _ = _make_loader()
        loader._export_type = "unsupported"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="Unexpected export type"):
            _ = [doc async for doc in loader.alazy_load()]


# ---------------------------------------------------------------------------
# DoclingServeAPIDocumentConverter — errors field handling
# ---------------------------------------------------------------------------


_DOCLING_ERRORS = [
    {
        "component_type": "user_input",
        "module_name": "",
        "error_message": "File format not allowed: binary.exe",
    }
]


class TestDoclingServeAPIDocumentConverter:
    """Tests the HTTP response → exception translation inside convert/aconvert.

    The httpx clients are patched so no network calls are made.
    """

    def test_convert_raises_on_non_empty_errors(self) -> None:
        """convert() raises DoclingConversionError when the response has errors."""
        converter = DoclingServeAPIDocumentConverter(base_url="http://docling-test")
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"errors": _DOCLING_ERRORS, "document": None}

        with patch.object(converter.client, "post", return_value=mock_response):
            with pytest.raises(DoclingConversionError, match="File format not allowed"):
                converter.convert(("binary.exe", b"\x00", "application/octet-stream"))

    def test_convert_error_carries_errors_list(self) -> None:
        """The DoclingConversionError carries the raw errors list from Docling."""
        converter = DoclingServeAPIDocumentConverter(base_url="http://docling-test")
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"errors": _DOCLING_ERRORS, "document": None}

        with patch.object(converter.client, "post", return_value=mock_response):
            with pytest.raises(DoclingConversionError) as exc_info:
                converter.convert(("binary.exe", b"\x00", "application/octet-stream"))
            assert exc_info.value.errors == _DOCLING_ERRORS

    @pytest.mark.asyncio
    async def test_aconvert_raises_on_non_empty_errors(self) -> None:
        """aconvert() raises DoclingConversionError when the response has errors."""
        converter = DoclingServeAPIDocumentConverter(base_url="http://docling-test")
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"errors": _DOCLING_ERRORS, "document": None}

        with patch.object(
            converter.async_client, "post", new=AsyncMock(return_value=mock_response)
        ):
            with pytest.raises(DoclingConversionError, match="File format not allowed"):
                await converter.aconvert(
                    ("binary.exe", b"\x00", "application/octet-stream")
                )
