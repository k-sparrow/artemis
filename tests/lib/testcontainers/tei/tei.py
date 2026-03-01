# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Text Embeddings Inference (TEI) testcontainer."""

from __future__ import annotations

from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

from src.lib.core.adapters.document_compressors.huggingface._client import (
    AsyncInferenceClient,
    InferenceClient,
)


__all__ = ["TEIContainer"]


class TEIContainer(DockerContainer):
    """Testcontainer for HuggingFace Text Embeddings Inference (TEI).

    TEI exposes an HTTP API on port 80.  The container loads a single model
    at startup; the model ID is passed as the ``--model-id`` CLI flag.

    On first run the container downloads model weights from the HuggingFace
    Hub, which can be slow.  Mount a host cache directory to avoid repeated
    downloads across test sessions::

        container.with_hf_cache(str(Path.home() / ".cache" / "huggingface"))

    Args:
        model_id: HuggingFace Hub model ID to load (e.g.
            ``"BAAI/bge-reranker-base"`` for reranking or
            ``"sentence-transformers/msmarco-MiniLM-L-12-v3"`` for
            embeddings).
        image: TEI Docker image.

    Example — embeddings::

        from tests.lib.testcontainers.tei import TEIContainer

        container = TEIContainer("sentence-transformers/msmarco-MiniLM-L-12-v3")
        container.start()

        client = container.client()
        embeddings = client.feature_extraction("Hello world")
        container.stop()

    Example — reranking::

        container = TEIContainer("BAAI/bge-reranker-base")
        container.start()

        client = container.client()
        results = client.text_reranking(
            texts=["doc one", "doc two"],
            query="what is this about?",
        )
        container.stop()
    """

    HTTP_PORT = 80
    DEFAULT_IMAGE = "ghcr.io/huggingface/text-embeddings-inference:cpu-1.8"
    _CONTAINER_CACHE_DIR = "/data"

    def __init__(
        self,
        model_id: str,
        image: str = DEFAULT_IMAGE,
    ) -> None:
        super().__init__(image)
        self._model_id = model_id

        self.with_command(f'--model-id "{model_id}"')
        self.with_exposed_ports(self.HTTP_PORT)

        # Wait until the model is fully loaded and the HTTP server is ready.
        # TEI returns 200 on GET / once the model is initialised.
        self.waiting_for(HttpWaitStrategy(self.HTTP_PORT, "/").for_status_code(200))

    @property
    def model_id(self) -> str:
        """The HuggingFace Hub model ID loaded by this container."""
        return self._model_id

    def with_hf_cache(self, host_cache_dir: str) -> Self:
        """Mount a host-side HuggingFace cache to avoid re-downloading weights.

        Args:
            host_cache_dir: Absolute path on the host to the HuggingFace cache
                directory (e.g. ``str(Path.home() / ".cache" / "huggingface")``).
        """
        self.with_volume_mapping(host_cache_dir, self._CONTAINER_CACHE_DIR, "rw")
        self.with_command(
            f'--model-id "{self._model_id}"'
            f" --huggingface-hub-cache {self._CONTAINER_CACHE_DIR}"
        )
        return self

    def get_url(self) -> str:
        """Return the TEI HTTP base URL reachable from the test process."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"

    def client(self) -> InferenceClient:
        """Return a synchronous InferenceClient connected to this container.

        The client is the patched variant from
        ``src.lib.core.adapters.document_compressors.huggingface._client``
        which extends the upstream ``huggingface_hub.InferenceClient`` with a
        ``text_reranking()`` method for cross-encoder reranking via TEI.

        Returns:
            A ready-to-use ``InferenceClient`` pointed at ``get_url()``.
        """
        return InferenceClient(model=self.get_url())

    def async_client(self) -> AsyncInferenceClient:
        """Return an async InferenceClient connected to this container.

        The client is the patched variant from
        ``src.lib.core.adapters.document_compressors.huggingface._client``
        which extends the upstream ``huggingface_hub.AsyncInferenceClient``
        with an async ``text_reranking()`` method.

        Returns:
            A ready-to-use ``AsyncInferenceClient`` pointed at ``get_url()``.
        """
        return AsyncInferenceClient(model=self.get_url())
