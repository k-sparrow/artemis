"""Document loaders for various document formats and services.

This module provides document loaders that integrate with external services
or handle specific document formats.
"""

__all__ = [
    "DoclingAPIServeLoader",
    "DoclingServeAPIDocumentConverter",
    "DEFAULT_CONVERTER_KWARGS",
]

from src.lib.core.adapters.loaders.docling import (
    DoclingAPIServeLoader,
    DoclingServeAPIDocumentConverter,
    DEFAULT_CONVERTER_KWARGS,
)
