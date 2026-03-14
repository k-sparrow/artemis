# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""LangChain document loader backed by PyMuPDF4LLM.

Converts a document (supplied as raw bytes) to Markdown using
``pymupdf4llm.to_markdown`` and yields it as a single
:class:`~langchain_core.documents.Document`.  No external service is
required — processing is fully in-process.
"""

# core
from typing import Dict, Any, Iterator
from pathlib import Path

# third party
from pymupdf4llm import to_markdown
from langchain_core.documents import Document
from langchain_core.document_loaders.base import BaseLoader

_all_ = ["PyMuPDF4LLMLoader"]


class PyMuPDF4LLMLoader(BaseLoader):
    """
    A document loader wrapper for PyMuPDF4LLM.

    Core implementation is taken from here:
    https://github.com/langchain-ai/langchain/discussions/22263#discussioncomment-9969296
    """

    def __init__(self, file_path: str | Path):
        """Initialize the loader with a file path.

        Args:
            file_path: The path to the file to load.
        """
        self.file_path = Path(file_path)

    def lazy_load(self) -> Iterator[Document]:
        """
        Lazy loads the PDF using pymupdf4llm and returns a list of documents
        each corresponding to a page of the document, converted to markdown
        """
        markdown_pages = []
        try:
            # Convert the PDF to markdown
            markdown_pages = to_markdown(self.file_path, page_chunks=True)
        except Exception as e:
            raise e

        # Extract the text content and metadata from the LlamaIndex dictionary
        # and use them to construct a Document object
        for page in markdown_pages:
            text: str = page["text"]  # type: ignore
            metadata: Dict[str, Any] = page["metadata"]  # type: ignore

            # Create a new Document object for the LangChain output
            yield Document(
                page_content=text,
                metadata={
                    "source": metadata["file_path"],
                    "file_path": metadata["file_path"],
                    "page": metadata["page"],
                    "total_pages": metadata["page_count"],
                    "format": metadata["format"],
                    "title": metadata["title"],
                    "author": metadata["author"],
                    "subject": metadata["subject"],
                    "keywords": metadata["keywords"],
                    "creator": metadata["creator"],
                    "producer": metadata["producer"],
                    "creationDate": metadata["creationDate"],
                    "modDate": metadata["modDate"],
                    "trapped": metadata["trapped"],
                },
            )
