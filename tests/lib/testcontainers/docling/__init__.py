# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""docling-serve testcontainers.

Provides three fixtures over the docling-serve Docker image:

- ``DoclingServeContainer`` — single container, local (sequential) engine.
- ``DoclingServeRayCluster`` — redis + ray-head + ray-worker + docling-serve
  cluster, Ray engine with server-side PDF page-slice fan-out (Epic 21).
- ``DoclingServeRayClusterWithApp`` — the same Ray cluster (patched image, see
  tools/oci/images/docling) plus MinIO and the real parsing-service app
  container, for testing S3-direct dispatch end-to-end.

The first two expose the same ``get_url()`` surface so callers can swap
between them.

Usage::

    from tests.lib.testcontainers.docling import DoclingServeContainer

    container = DoclingServeContainer()
    container.start()

    url = container.get_url()   # e.g. "http://localhost:32768"
    container.stop()
"""

from tests.lib.testcontainers.docling.docling import DoclingServeContainer
from tests.lib.testcontainers.docling.docling_ray import DoclingServeRayCluster
from tests.lib.testcontainers.docling.docling_ray_app import (
    DoclingServeRayClusterWithApp,
)

__all__ = [
    "DoclingServeContainer",
    "DoclingServeRayCluster",
    "DoclingServeRayClusterWithApp",
]
