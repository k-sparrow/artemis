# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""vLLM testcontainer.

Provides a ``VLLMContainer`` that wraps the vLLM OpenAI-compatible Docker
image and exposes the base URL of the running server.

Usage::

    from tests.lib.testcontainers.vllm import VLLMContainer
    from pathlib import Path

    container = VLLMContainer("colbert-ir/colbertv2.0")
    container.with_hf_cache(str(Path.home() / ".cache" / "huggingface"))
    container.start()

    url = container.get_url()   # e.g. "http://localhost:32768"
    container.stop()
"""

from tests.lib.testcontainers.vllm.vllm import VLLMContainer

__all__ = ["VLLMContainer"]
