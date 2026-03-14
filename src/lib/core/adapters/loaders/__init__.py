"""Document loaders for various document formats and services.

This module provides document loaders that integrate with external services
or handle specific document formats.
"""

from src.lib.core.adapters.loaders.docling import (
    DoclingAPIServeLoader,
    DoclingServeAPIDocumentConverter,
    DEFAULT_CONVERTER_KWARGS,
)

# from src.lib.core.adapters.loaders.pymupdf4llm_ import PyMuPDF4LLMLoader
from src.lib.core.adapters.loaders.config import (
    LoaderFactory,
    LoaderType,
    LoaderConfig,
    create_loader_factory,
)


__all__ = [
    "DoclingAPIServeLoader",
    "DoclingServeAPIDocumentConverter",
    "DEFAULT_CONVERTER_KWARGS",
    "PyMuPDF4LLMLoader",
    "LoaderFactory",
    "LoaderType",
    "LoaderConfig",
    "create_loader_factory",
]
