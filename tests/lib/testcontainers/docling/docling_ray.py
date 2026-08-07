# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Ray-backed docling-serve cluster testcontainer (server-side PDF page-slice fan-out)."""

from __future__ import annotations

from pathlib import Path

from testcontainers.compose import DockerCompose
from typing_extensions import Self

from tests.lib.polling import wait_for_http

__all__ = ["DoclingServeRayCluster"]

_COMPOSE_DIR = Path(__file__).parent
_COMPOSE_FILE = "docker-compose.ray.yaml"
_DOCLING_SERVICE = "docling-serve"
_DOCLING_PORT = 5001
_STARTUP_TIMEOUT_S = 300


class DoclingServeRayCluster:
    """testcontainers.compose fixture for docling-serve's Ray engine.

    A single ``DockerContainer`` can't model this — the Ray engine needs a small
    cluster (redis + ray-head + ray-worker) fronting docling-serve. Wraps
    ``testcontainers.compose.DockerCompose`` over the standalone
    ``docker-compose.ray.yaml`` in this directory (see TODOs.md Epic 21).
    Exposes the same ``get_url()`` surface as ``DoclingServeContainer`` so
    callers can swap between the single-container local engine and this
    cluster with no other code changes.

    Example::

        from tests.lib.testcontainers.docling import DoclingServeRayCluster

        with DoclingServeRayCluster() as cluster:
            url = cluster.get_url()  # "http://localhost:<port>"
    """

    def __init__(self) -> None:
        self._compose = DockerCompose(
            context=str(_COMPOSE_DIR),
            compose_file_name=[_COMPOSE_FILE],
            pull=False,
            build=False,
        )

    def start(self) -> Self:
        self._compose.start()
        try:
            wait_for_http(f"{self.get_url()}/health", timeout=_STARTUP_TIMEOUT_S)
        except Exception:
            self.stop()
            raise
        return self

    def stop(self) -> None:
        self._compose.stop()

    def get_url(self) -> str:
        """Return the docling-serve HTTP base URL reachable from the test process."""
        host, port = self._compose.get_service_host_and_port(
            _DOCLING_SERVICE, _DOCLING_PORT
        )
        return f"http://{host}:{port}"

    def get_logs(self, service: str) -> tuple[str, str]:
        """Return (stdout, stderr) for one of the cluster's services (redis,
        ray-head, ray-worker, docling-serve) — for dumping on test failure,
        since the stack is gone by the time a caller sees an assertion error."""
        return self._compose.get_logs(service)

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
