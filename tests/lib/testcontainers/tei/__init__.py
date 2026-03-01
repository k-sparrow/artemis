# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Text Embeddings Inference (TEI) testcontainer.

Provides a ``TEIContainer`` that wraps the HuggingFace TEI Docker image and
exposes sync and async ``InferenceClient`` instances (the patched variant that
adds ``text_reranking()``) pointed at the running container.

Usage::

    from tests.lib.testcontainers.tei import TEIContainer

    container = TEIContainer("BAAI/bge-reranker-base")
    container.start()

    # Sync reranking
    client = container.client()
    results = client.text_reranking(texts=["doc one", "doc two"], query="query")

    # Async embeddings
    async_client = container.async_client()
    embeddings = await async_client.feature_extraction("Hello world")

    container.stop()
"""

from tests.lib.testcontainers.tei.tei import TEIContainer

__all__ = ["TEIContainer"]
