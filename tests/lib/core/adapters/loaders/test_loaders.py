# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for loader configuration and PyMuPDF4LLMLoader.

All tests are pure unit tests — no external services, no filesystem side
effects (to_markdown is mocked).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from langchain_core.documents import Document

from src.lib.core.adapters.loaders.config import (
    LoaderConfig,
    LoaderType,
    create_loader_factory,
)
from src.lib.core.adapters.loaders.pymupdf4llm_ import PyMuPDF4LLMLoader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_PAGES = [
    {
        "text": "Page one content",
        "metadata": {
            "page": 0,
            "page_count": 2,
            "format": "PDF 1.4",
            "title": "Test Doc",
            "author": "Alice",
        },
    },
    {
        "text": "Page two content",
        "metadata": {
            "page": 1,
            "page_count": 2,
            "format": "PDF 1.4",
            "title": "Test Doc",
            "author": "Alice",
        },
    },
]


# ---------------------------------------------------------------------------
# LoaderConfig
# ---------------------------------------------------------------------------


class TestLoaderConfig:
    """LoaderConfig is a frozen dataclass — verify mutability and equality."""

    def test_frozen(self) -> None:
        cfg = LoaderConfig(loader_type=LoaderType.PYMUPDF4LLM)
        with pytest.raises((AttributeError, TypeError)):
            cfg.loader_type = "other"  # type: ignore[misc]

    def test_equality(self) -> None:
        a = LoaderConfig(loader_type=LoaderType.PYMUPDF4LLM)
        b = LoaderConfig(loader_type=LoaderType.PYMUPDF4LLM)
        assert a == b


# ---------------------------------------------------------------------------
# create_loader_factory
# ---------------------------------------------------------------------------


class TestCreateLoaderFactory:
    """create_loader_factory selects the correct loader class per LoaderType."""

    def test_pymupdf4llm_factory_returns_loader(self) -> None:
        """PYMUPDF4LLM config → factory constructs a PyMuPDF4LLMLoader."""
        cfg = LoaderConfig(loader_type=LoaderType.PYMUPDF4LLM)
        factory = create_loader_factory(cfg)
        loader = factory(b"%PDF", "doc.pdf", "application/pdf")
        assert isinstance(loader, PyMuPDF4LLMLoader)

    def test_unknown_loader_type_raises(self) -> None:
        """An unrecognised LoaderType raises ValueError."""
        cfg = LoaderConfig(loader_type="unknown_type")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="Unknown loader type"):
            create_loader_factory(cfg)

    def test_pymupdf4llm_factory_forwards_args(self) -> None:
        """The loader constructed by the factory carries the supplied args."""
        cfg = LoaderConfig(loader_type=LoaderType.PYMUPDF4LLM)
        factory = create_loader_factory(cfg)
        loader = factory(b"bytes", "report.pdf", "application/pdf")
        assert isinstance(loader, PyMuPDF4LLMLoader)
        assert loader._file == b"bytes"
        assert loader._filename == "report.pdf"
        assert loader._content_type == "application/pdf"


# ---------------------------------------------------------------------------
# PyMuPDF4LLMLoader
# ---------------------------------------------------------------------------


class TestPyMuPDF4LLMLoader:
    """Unit tests for PyMuPDF4LLMLoader — to_markdown is always mocked."""

    def test_lazy_load_yields_one_document_per_page(self) -> None:
        """Each page dict from to_markdown becomes one Document."""
        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="test.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            return_value=_FAKE_PAGES,
        ):
            docs = list(loader.lazy_load())

        assert len(docs) == 2
        assert all(isinstance(d, Document) for d in docs)

    def test_page_content_matches_text_field(self) -> None:
        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="test.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            return_value=_FAKE_PAGES,
        ):
            docs = list(loader.lazy_load())

        assert docs[0].page_content == "Page one content"
        assert docs[1].page_content == "Page two content"

    def test_source_metadata_is_filename(self) -> None:
        """source should reflect the logical filename, not the temp file path."""
        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="my_report.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            return_value=_FAKE_PAGES,
        ):
            docs = list(loader.lazy_load())

        assert docs[0].metadata["source"] == "my_report.pdf"
        assert docs[1].metadata["source"] == "my_report.pdf"

    def test_page_number_metadata(self) -> None:
        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="doc.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            return_value=_FAKE_PAGES,
        ):
            docs = list(loader.lazy_load())

        assert docs[0].metadata["page"] == 0
        assert docs[1].metadata["page"] == 1
        assert docs[0].metadata["total_pages"] == 2

    def test_empty_document_yields_no_docs(self) -> None:
        """A file with no pages produces an empty iterator."""
        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="empty.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            return_value=[],
        ):
            docs = list(loader.lazy_load())

        assert docs == []

    def test_tmp_file_is_deleted_on_success(self) -> None:
        """The temporary file must be removed even on a successful parse."""
        import os

        captured_paths: list[str] = []

        original_to_markdown = __import__(  # noqa: F841
            "pymupdf4llm", fromlist=["to_markdown"]
        ).to_markdown

        def capturing_to_markdown(path: str, **kwargs):  # type: ignore[no-untyped-def]
            captured_paths.append(path)
            return _FAKE_PAGES

        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="doc.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            side_effect=capturing_to_markdown,
        ):
            list(loader.lazy_load())

        assert captured_paths, "to_markdown was not called"
        assert not os.path.exists(captured_paths[0]), "temp file was not deleted"

    def test_tmp_file_is_deleted_on_error(self) -> None:
        """The temporary file must be removed even when to_markdown raises."""
        import os

        captured_paths: list[str] = []

        def failing_to_markdown(path: str, **kwargs):  # type: ignore[no-untyped-def]
            captured_paths.append(path)
            raise RuntimeError("parse error")

        loader = PyMuPDF4LLMLoader(
            file=b"%PDF", filename="doc.pdf", content_type="application/pdf"
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            side_effect=failing_to_markdown,
        ):
            with pytest.raises(RuntimeError, match="parse error"):
                list(loader.lazy_load())

        assert captured_paths, "to_markdown was not called"
        assert not os.path.exists(captured_paths[0]), "temp file leaked on error"

    def test_suffix_derived_from_filename(self) -> None:
        """Temp file suffix matches the input filename's extension."""
        captured_paths: list[str] = []

        def capturing_to_markdown(path: str, **kwargs):  # type: ignore[no-untyped-def]
            captured_paths.append(path)
            return []

        loader = PyMuPDF4LLMLoader(
            file=b"data",
            filename="presentation.docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # noqa: E501
        )
        with patch(
            "src.lib.core.adapters.loaders.pymupdf4llm_.to_markdown",
            side_effect=capturing_to_markdown,
        ):
            list(loader.lazy_load())

        assert captured_paths[0].endswith(".docx")
