# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""docling-serve testcontainer."""

from __future__ import annotations

import docker
from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

__all__ = ["DoclingServeContainer"]

_HTTP_PORT = 5001
_DEFAULT_IMAGE = "ghcr.io/docling-project/docling-serve-cu128:v1.29.0"
_STARTUP_TIMEOUT_S = 300


class DoclingServeContainer(DockerContainer):
    """Testcontainer for docling-serve (async document parsing).

    Requires a CUDA-capable GPU and the Docker NVIDIA runtime by default —
    matches the ``docling-serve-cu128`` image used in dev-compose. Pass
    ``gpu=False`` with the CPU image (``ghcr.io/docling-project/docling-serve:main``)
    for environments without a GPU.

    Example::

        from tests.lib.testcontainers.docling import DoclingServeContainer

        container = DoclingServeContainer()
        container.start()

        url = container.get_url()  # "http://localhost:<port>"
        container.stop()
    """

    HTTP_PORT = _HTTP_PORT

    def __init__(
        self,
        image: str = _DEFAULT_IMAGE,
        gpu: bool = True,
    ) -> None:
        super().__init__(image)

        self.with_exposed_ports(self.HTTP_PORT)
        self.with_env("DOCLING_SERVE_ENABLE_UI", "true")
        self.with_env("DOCLING_SERVE_MAX_SYNC_WAIT", "12")
        if gpu:
            self.with_env("NVIDIA_VISIBLE_DEVICES", "all")
            self.with_kwargs(
                device_requests=[
                    docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
                ],
            )
        self.waiting_for(
            HttpWaitStrategy(self.HTTP_PORT, "/health")
            .for_status_code(200)
            .with_startup_timeout(_STARTUP_TIMEOUT_S)
        )

    def with_hf_cache(self, host_cache_dir: str) -> Self:
        """Mount a host-side HuggingFace cache to avoid re-downloading model weights."""
        self.with_volume_mapping(host_cache_dir, "/mnt/models", "rw")
        self.with_env("DOCLING_SERVE_ARTIFACTS_PATH", "/mnt/models")
        return self

    def get_url(self) -> str:
        """Return the docling-serve HTTP base URL reachable from the test process."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"
