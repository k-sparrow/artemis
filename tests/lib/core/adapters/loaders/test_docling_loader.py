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
    PAGE_BREAK_PLACEHOLDER,
    DoclingAPIServeLoader,
    DoclingConversionError,
    DoclingServeAPIDocumentConverter,
    ExportType,
    MetaExtractor,
    split_pages,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_MARKDOWN_FILE: tuple[str, bytes, str] = (
    "test.md",
    b"# Hello\n\nThis is a test document with some content.\n",
    "text/markdown",
)


def _make_chunk(labels: list[DocItemLabel], page_no: int | None = None) -> MagicMock:
    """Build a minimal fake BaseChunk with the given labels and provenance page.

    Every doc item carries an explicit ``prov`` list (empty when ``page_no`` is
    ``None``) so ``_first_page_no`` iterates a real list rather than a MagicMock.
    """
    chunk = MagicMock()
    prov = [MagicMock(page_no=page_no)] if page_no is not None else []
    chunk.meta.doc_items = [MagicMock(label=label, prov=list(prov)) for label in labels]
    return chunk


def _make_dl_doc(
    markdown: str = "# Doc\n\nContent.", pages: tuple[int, ...] = (1,)
) -> MagicMock:
    """Build a minimal fake DoclingDocument.

    ``export_to_markdown`` returns ``markdown`` regardless of kwargs (MagicMock),
    and ``.pages`` is a page-number-keyed dict so ``split_pages`` can sort it.
    """
    dl_doc = MagicMock()
    dl_doc.export_to_markdown.return_value = markdown
    dl_doc.pages = {p: MagicMock() for p in pages}
    return dl_doc


def _make_loader(
    export_type: ExportType = ExportType.DOC_CHUNKS,
    chunks: list[MagicMock] | None = None,
    markdown: str = "# Doc\n\nContent.",
    pages: tuple[int, ...] = (1,),
) -> tuple[DoclingAPIServeLoader, MagicMock, MagicMock]:
    """Return (loader, mock_converter, mock_chunker)."""
    dl_doc = _make_dl_doc(markdown, pages=pages)

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

    def test_dl_meta_is_not_emitted(self) -> None:
        """dl_meta is stripped at the boundary — it must never reach the wire."""
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.TEXT])
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert "dl_meta" not in meta

    def test_page_no_from_first_provenance(self) -> None:
        """page_no is the first doc item's first provenance page."""
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.TEXT], page_no=5)
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["page_no"] == 5

    def test_page_no_is_none_without_provenance(self) -> None:
        """A chunk with no provenance yields page_no None (links no parent)."""
        extractor = MetaExtractor()
        chunk = _make_chunk([DocItemLabel.TEXT])
        meta = extractor.extract_chunk_meta("doc.pdf", chunk)
        assert meta["page_no"] is None

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


# ---------------------------------------------------------------------------
# split_pages — single-pass per-page Markdown reconstruction
# ---------------------------------------------------------------------------


class TestSplitPages:
    """Page reconstruction from a single whole-document Markdown export."""

    @staticmethod
    def _doc(markdown: str, pages: tuple[int, ...]) -> MagicMock:
        return _make_dl_doc(markdown, pages=pages)

    def test_splits_on_placeholder_and_maps_sorted_pages(self) -> None:
        md = f"page one{PAGE_BREAK_PLACEHOLDER}page two"
        result = split_pages(self._doc(md, (1, 2)))
        assert result == [(1, "page one"), (2, "page two")]

    def test_uses_page_break_placeholder_kwarg(self) -> None:
        doc = self._doc("only", (1,))
        split_pages(doc)
        doc.export_to_markdown.assert_called_once_with(
            page_break_placeholder=PAGE_BREAK_PLACEHOLDER
        )

    def test_segments_are_stripped(self) -> None:
        md = f"  spaced  {PAGE_BREAK_PLACEHOLDER}\n\nnext\n"
        assert split_pages(self._doc(md, (1, 2))) == [(1, "spaced"), (2, "next")]

    def test_non_contiguous_page_numbers_preserved(self) -> None:
        md = f"a{PAGE_BREAK_PLACEHOLDER}b"
        assert split_pages(self._doc(md, (3, 7))) == [(3, "a"), (7, "b")]

    def test_single_page_has_no_placeholder(self) -> None:
        assert split_pages(self._doc("just one page", (1,))) == [(1, "just one page")]

    def test_count_mismatch_truncates_to_shorter(self) -> None:
        # Fewer segments than pages (an empty page emits no transition): zip
        # truncates, mapping the available segments to the leading page numbers.
        assert split_pages(self._doc("single", (1, 2, 3))) == [(1, "single")]

    def test_non_paginated_doc_becomes_single_page(self) -> None:
        # Markdown/HTML/text have an empty page index but real content → one
        # synthetic page so the document still gets a parent.
        assert split_pages(self._doc("# Hello\n\nWorld", ())) == [
            (1, "# Hello\n\nWorld")
        ]

    def test_empty_non_paginated_doc_yields_no_pages(self) -> None:
        # No page index AND no content → no page at all.
        assert split_pages(self._doc("", ())) == []


# ---------------------------------------------------------------------------
# DoclingAPIServeLoader.aload_artifact — chunks + pages + dl_doc in one pass
# ---------------------------------------------------------------------------


class TestAloadArtifact:
    """The artifact path: one conversion yields chunk docs, pages, and dl_doc."""

    @pytest.mark.asyncio
    async def test_returns_chunk_docs_pages_and_dl_doc(self) -> None:
        chunk = _make_chunk([DocItemLabel.TEXT], page_no=2)
        loader, converter, _ = _make_loader(
            chunks=[chunk],
            markdown=f"first{PAGE_BREAK_PLACEHOLDER}second",
            pages=(1, 2),
        )

        chunk_docs, pages, dl_doc = await loader.aload_artifact()

        assert [d.page_content for d in chunk_docs] == ["chunk text"]
        assert chunk_docs[0].metadata["page_no"] == 2
        assert "dl_meta" not in chunk_docs[0].metadata
        assert pages == [(1, "first"), (2, "second")]
        assert dl_doc is converter.aconvert.return_value

    @pytest.mark.asyncio
    async def test_converts_exactly_once(self) -> None:
        loader, converter, _ = _make_loader(chunks=[_make_chunk([DocItemLabel.TEXT])])
        await loader.aload_artifact()
        converter.aconvert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_chunks_regardless_of_export_type(self) -> None:
        # export_type=MARKDOWN must not suppress chunking on the artifact path.
        chunk = _make_chunk([DocItemLabel.TEXT], page_no=1)
        loader, _, chunker = _make_loader(
            export_type=ExportType.MARKDOWN, chunks=[chunk]
        )
        chunk_docs, _, _ = await loader.aload_artifact()
        assert len(chunk_docs) == 1
        chunker.chunk.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_document_yields_no_chunks(self) -> None:
        loader, _, _ = _make_loader(chunks=[], markdown="only page", pages=(1,))
        chunk_docs, pages, _ = await loader.aload_artifact()
        assert chunk_docs == []
        assert pages == [(1, "only page")]
