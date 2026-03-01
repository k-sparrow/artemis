"""Adapters for external services used in RAG pipelines.

This module contains pluggable adapters for:
- Document loaders (Docling, Unstructured, etc.)
- Embedding models (HuggingFace TEI, OpenAI, etc.)
- Vector stores (Qdrant, Weaviate, etc.)
- Rerankers (Cohere, cross-encoders, etc.)

These are implementation details that can be swapped without affecting
the core ingestion and retrieval logic.
"""

__all__ = [
    # Submodules
    "loaders",
    "embedding",
    "vectorstore",
]
