# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for DoclingAPIServeLoader against a real Docling container.

Requires the ``docling_base_url`` session fixture from conftest.py, which
starts a ``quay.io/docling-project/docling-serve:latest`` (CPU) container
via testcontainers.

The container is started once per session and shared across all tests.
Tests are independent — no shared state, no side effects.

TestDoclingAPIServeLoaderIntegration
    Exercises ``lazy_load`` (sync) and ``alazy_load`` (async) for both
    ExportType.DOC_CHUNKS and ExportType.MARKDOWN with a real markdown
    document payload.  Asserts Document shape, metadata, and non-empty
    page_content.
"""

from __future__ import annotations

import pytest
from langchain_core.documents import Document

from src.lib.core.adapters.loaders.docling import (
    DEFAULT_CONVERTER_KWARGS,
    DoclingAPIServeLoader,
    DoclingServeAPIDocumentConverter,
    ExportType,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

# A markdown document rich enough for Docling to produce at least one chunk.
_MARKDOWN_FILE: tuple[str, bytes, str] = (
    "test.md",
    (
        b"# Introduction\n\n"
        b"This is a test document used to verify that the Docling loader "
        b"correctly parses content and returns well-formed LangChain Documents.\n\n"
        b"## Section One\n\n"
        b"Artemis is a RAG document ingestion system with an event-driven "
        b"microservices architecture built on Kafka, Celery, Qdrant, and Docling.\n"
    ),
    "text/markdown",
)


def _make_loader(
    docling_base_url: str,
    export_type: ExportType = ExportType.DOC_CHUNKS,
    source: tuple[str, bytes, str] = _MARKDOWN_FILE,
) -> DoclingAPIServeLoader:
    converter = DoclingServeAPIDocumentConverter(
        base_url=docling_base_url, **DEFAULT_CONVERTER_KWARGS
    )
    return DoclingAPIServeLoader(
        source=source,
        converter=converter,
        export_type=export_type,
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDoclingAPIServeLoaderIntegration:
    """Real Docling container — verifies end-to-end document loading."""

    # --- lazy_load / DOC_CHUNKS ---

    def test_lazy_load_doc_chunks_returns_documents(
        self, docling_base_url: str
    ) -> None:
        """Docling parses the markdown and returns at least one Document."""
        loader = _make_loader(docling_base_url)
        docs = list(loader.lazy_load())
        assert len(docs) >= 1
        assert all(isinstance(d, Document) for d in docs)

    def test_lazy_load_doc_chunks_page_content_non_empty(
        self, docling_base_url: str
    ) -> None:
        loader = _make_loader(docling_base_url)
        docs = list(loader.lazy_load())
        assert all(len(d.page_content) > 0 for d in docs)

    def test_lazy_load_source_metadata_matches_filename(
        self, docling_base_url: str
    ) -> None:
        """Every Document carries ``source`` == the original filename."""
        loader = _make_loader(docling_base_url)
        docs = list(loader.lazy_load())
        assert all(d.metadata["source"] == "test.md" for d in docs)

    def test_lazy_load_type_metadata_present_on_every_doc(
        self, docling_base_url: str
    ) -> None:
        """DOC_CHUNKS mode: every Document has a ``type`` metadata field."""
        loader = _make_loader(docling_base_url)
        docs = list(loader.lazy_load())
        assert all("type" in d.metadata for d in docs)

    def test_lazy_load_dl_meta_present_on_every_doc(
        self, docling_base_url: str
    ) -> None:
        """DOC_CHUNKS mode: every Document has a ``dl_meta`` metadata field."""
        loader = _make_loader(docling_base_url)
        docs = list(loader.lazy_load())
        assert all("dl_meta" in d.metadata for d in docs)

    # --- lazy_load / MARKDOWN ---

    def test_lazy_load_markdown_yields_exactly_one_document(
        self, docling_base_url: str
    ) -> None:
        """MARKDOWN export always produces exactly one Document."""
        loader = _make_loader(docling_base_url, export_type=ExportType.MARKDOWN)
        docs = list(loader.lazy_load())
        assert len(docs) == 1

    def test_lazy_load_markdown_page_content_contains_heading(
        self, docling_base_url: str
    ) -> None:
        """The exported Markdown should contain the document heading."""
        loader = _make_loader(docling_base_url, export_type=ExportType.MARKDOWN)
        docs = list(loader.lazy_load())
        assert "Introduction" in docs[0].page_content

    def test_lazy_load_markdown_source_metadata(self, docling_base_url: str) -> None:
        loader = _make_loader(docling_base_url, export_type=ExportType.MARKDOWN)
        docs = list(loader.lazy_load())
        assert docs[0].metadata["source"] == "test.md"

    # --- alazy_load / DOC_CHUNKS ---

    @pytest.mark.asyncio
    async def test_alazy_load_doc_chunks_returns_documents(
        self, docling_base_url: str
    ) -> None:
        loader = _make_loader(docling_base_url, export_type=ExportType.DOC_CHUNKS)
        docs = [doc async for doc in loader.alazy_load()]
        assert len(docs) >= 1
        assert all(isinstance(d, Document) for d in docs)

    @pytest.mark.asyncio
    async def test_alazy_load_source_metadata_matches_filename(
        self, docling_base_url: str
    ) -> None:
        loader = _make_loader(docling_base_url, export_type=ExportType.DOC_CHUNKS)
        docs = [doc async for doc in loader.alazy_load()]
        assert all(d.metadata["source"] == "test.md" for d in docs)

    # --- alazy_load / MARKDOWN ---

    @pytest.mark.asyncio
    async def test_alazy_load_markdown_yields_one_document(
        self, docling_base_url: str
    ) -> None:
        loader = _make_loader(docling_base_url, export_type=ExportType.MARKDOWN)
        docs = [doc async for doc in loader.alazy_load()]
        assert len(docs) == 1
        assert isinstance(docs[0], Document)
