# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""vLLM testcontainer for ColBERT late-interaction embedding models."""

from __future__ import annotations

import json

import docker
from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

__all__ = ["VLLMContainer"]

_HTTP_PORT = 8000
_DEFAULT_IMAGE = "vllm/vllm-openai:v0.20.2"
_CONTAINER_CACHE_DIR = "/root/.cache/huggingface"
_STARTUP_TIMEOUT_S = 600


class VLLMContainer(DockerContainer):
    """Testcontainer for a vLLM server running a ColBERT late-interaction model.

    The vLLM image ENTRYPOINT is ``["vllm", "serve"]``; only the model name
    and flags are passed as CMD.  The ``/pooling`` endpoint is used for
    token-level embeddings (``--pooler-config.task token_embed``).

    The container requires a CUDA-capable GPU and the Docker NVIDIA runtime.
    Mount a host-side HuggingFace cache to avoid re-downloading model weights
    on every test run::

        container.with_hf_cache(str(Path.home() / ".cache" / "huggingface"))

    Args:
        model_id: HuggingFace Hub model ID, e.g. ``"colbert-ir/colbertv2.0"``.
        image: vLLM Docker image tag.

    Example::

        from tests.lib.testcontainers.vllm import VLLMContainer

        container = VLLMContainer("colbert-ir/colbertv2.0")
        container.with_hf_cache(str(Path.home() / ".cache" / "huggingface"))
        container.start()

        url = container.get_url()  # "http://localhost:<port>"
        container.stop()
    """

    HTTP_PORT = _HTTP_PORT

    def __init__(
        self,
        model_id: str,
        image: str = _DEFAULT_IMAGE,
        gpu_memory_utilization: float = 0.40,
    ) -> None:
        super().__init__(image)
        self._model_id = model_id
        self._extra_flags: list[str] = [
            f"--gpu-memory-utilization {gpu_memory_utilization}"
        ]

        self.with_exposed_ports(self.HTTP_PORT)
        self.with_kwargs(
            device_requests=[
                docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
            ],
            shm_size="10g",
            ipc_mode="host",
        )
        self.waiting_for(
            HttpWaitStrategy(self.HTTP_PORT, "/health")
            .for_status_code(200)
            .with_startup_timeout(_STARTUP_TIMEOUT_S)
        )
        self._apply_command()

    @property
    def model_id(self) -> str:
        return self._model_id

    def with_hf_cache(self, host_cache_dir: str) -> Self:
        """Mount a host-side HuggingFace cache to avoid re-downloading weights."""
        self.with_volume_mapping(host_cache_dir, _CONTAINER_CACHE_DIR, "rw")
        return self

    def with_trust_remote_code(self) -> Self:
        """
        Add --trust-remote-code (required by some models, e.g. jinaai/jina-colbert-v2).
        """
        self._extra_flags.append("--trust-remote-code")
        self._apply_command()
        return self

    def with_hf_overrides(self, overrides: dict) -> Self:
        """Add --hf-overrides for models with non-BERT backbones.

        Required when vLLM cannot infer the correct ColBERT architecture class
        from the model config alone, e.g.::

            container.with_hf_overrides({"architectures": ["ColBERTJinaRobertaModel"]})
        """
        self._extra_flags.append(f"--hf-overrides '{json.dumps(overrides)}'")
        self._apply_command()
        return self

    def get_url(self) -> str:
        """Return the vLLM HTTP base URL reachable from the test process."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"

    def _apply_command(self) -> None:
        parts = [self._model_id, "--pooler-config.task token_embed"] + self._extra_flags
        self.with_command(" ".join(parts))
