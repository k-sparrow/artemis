# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""LangChain document loader backed by PyMuPDF4LLM.

Converts a document (supplied as raw bytes) to Markdown using
``pymupdf4llm.to_markdown`` and yields one
:class:`~langchain_core.documents.Document` per page.  No external service
is required — processing is fully in-process.

The loader writes the bytes to a temporary file during :meth:`lazy_load`,
processes it, then deletes it before returning — callers never need to
manage a file themselves.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterator

from langchain_core.document_loaders.base import BaseLoader
from langchain_core.documents import Document
from pymupdf4llm import to_markdown

__all__ = ["PyMuPDF4LLMLoader"]


class PyMuPDF4LLMLoader(BaseLoader):
    """LangChain document loader wrapper for PyMuPDF4LLM.

    Accepts raw file bytes so it fits the :data:`LoaderFactory` contract
    ``(file: bytes, filename: str, content_type: str) → BaseLoader``.

    Args:
        file: Raw bytes of the document to parse.
        filename: Original file name (used as the ``source`` metadata field
            and to derive the temporary-file extension).
        content_type: MIME type of the document (unused by PyMuPDF4LLM
            itself, kept for interface uniformity with other loaders).
    """

    def __init__(self, file: bytes, filename: str, content_type: str) -> None:
        self._file = file
        self._filename = filename
        self._content_type = content_type

    def lazy_load(self) -> Iterator[Document]:
        """Write bytes to a temp file, convert to Markdown, clean up."""
        suffix = Path(self._filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(self._file)
            tmp_path = tmp.name
        try:
            markdown_pages = to_markdown(tmp_path, page_chunks=True)
        finally:
            os.unlink(tmp_path)

        for page in markdown_pages:
            meta = page["metadata"]
            yield Document(
                page_content=page["text"],
                metadata={
                    "source": self._filename,
                    "page": meta["page"],
                    "total_pages": meta["page_count"],
                    "format": meta["format"],
                    "title": meta["title"],
                    "author": meta["author"],
                },
            )
