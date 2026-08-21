# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Ray-backed docling-serve cluster + the real parsing-service app container +
MinIO, as one compose stack (server-side PDF page-slice fan-out AND S3-direct
dispatch, Epic 21)."""

from __future__ import annotations

from pathlib import Path

from minio import Minio
from testcontainers.compose import DockerCompose
from typing_extensions import Self

from tests.lib.polling import wait_for_http

__all__ = ["DoclingServeRayClusterWithApp"]

_COMPOSE_DIR = Path(__file__).parent
_COMPOSE_FILE = "docker-compose.ray-app.yaml"
_PARSING_SERVICE = "backend-parsing"
_PARSING_PORT = 10001
_MINIO_SERVICE = "minio"
_MINIO_PORT = 9000
_CLUSTER_SERVICES = (
    "redis",
    "ray-head",
    "ray-worker",
    "docling-serve",
    "backend-parsing",
)
_STARTUP_TIMEOUT_S = 300


class DoclingServeRayClusterWithApp:
    """testcontainers.compose fixture combining the Ray-engine docling-serve
    cluster with the real ``artemis/backend-parsing:dev`` app image and MinIO.

    ``DoclingServeRayCluster`` (docling_ray.py) exercises the Ray engine
    directly via ``DoclingParseClient``, bypassing the parsing service and S3
    entirely — it never touches the S3-source/batch codepath docling-serve's
    Ray-serde patch fixes (see tools/oci/images/docling). This fixture is for
    the one test that needs to: driving the parsing service's own
    ``source_ref`` (S3-direct) submit path against a real, Ray-engine
    docling-serve.

    Example::

        from tests.lib.testcontainers.docling import DoclingServeRayClusterWithApp

        with DoclingServeRayClusterWithApp() as cluster:
            url = cluster.get_parsing_url()
            minio_client = cluster.get_minio_client()
    """

    def __init__(self) -> None:
        self._compose = DockerCompose(
            context=str(_COMPOSE_DIR),
            compose_file_name=[_COMPOSE_FILE],
            pull=False,
            build=False,
        )

    def start(self) -> Self:
        try:
            self._compose.start()
            wait_for_http(
                f"{self.get_parsing_url()}/health/readiness",
                timeout=_STARTUP_TIMEOUT_S,
            )
        except Exception:
            # self._compose.start() itself can fail partway through (e.g. a
            # transient "docker compose up" error) — previously only
            # wait_for_http's failure triggered cleanup, so a startup
            # failure left orphaned containers under this fixture's
            # non-unique "docling" compose project name, which then made
            # every subsequent run collide with that stale state too
            # ("No such container: <id>" from docker compose up racing
            # leftover containers it didn't create this time).
            self.stop()
            raise
        return self

    def stop(self) -> None:
        self._compose.stop()

    def get_parsing_url(self) -> str:
        """Return the parsing service's HTTP base URL reachable from the test process."""
        host, port = self._compose.get_service_host_and_port(
            _PARSING_SERVICE, _PARSING_PORT
        )
        return f"http://{host}:{port}"

    def get_minio_client(self) -> Minio:
        """Host-side MinIO client for seeding source_ref input and reading
        back written artifacts."""
        host, port = self._compose.get_service_host_and_port(
            _MINIO_SERVICE, _MINIO_PORT
        )
        return Minio(
            f"{host}:{port}",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

    def get_logs(self, service: str) -> tuple[str, str]:
        """Return (stdout, stderr) for one of the stack's services (redis,
        ray-head, ray-worker, docling-serve, backend-parsing, minio) — for
        dumping on test failure, since the stack is gone by the time a caller
        sees an assertion error."""
        return self._compose.get_logs(service)

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
