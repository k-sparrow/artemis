"""Embedding model adapters for text-to-vector conversion.

This module provides adapters for various embedding services.
"""

__all__ = [
    "HuggingFaceEndpointEmbeddings",
]

from src.lib.core.adapters.embedding.huggingface import (
    HuggingFaceEndpointEmbeddings,
)
