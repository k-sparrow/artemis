# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""docling-serve testcontainers.

Provides two fixtures over the docling-serve Docker image:

- ``DoclingServeContainer`` — single container, local (sequential) engine.
- ``DoclingServeRayCluster`` — redis + ray-head + ray-worker + docling-serve
  cluster, Ray engine with server-side PDF page-slice fan-out (Epic 21).

Both expose the same ``get_url()`` surface so callers can swap between them.

Usage::

    from tests.lib.testcontainers.docling import DoclingServeContainer

    container = DoclingServeContainer()
    container.start()

    url = container.get_url()   # e.g. "http://localhost:32768"
    container.stop()
"""

from tests.lib.testcontainers.docling.docling import DoclingServeContainer
from tests.lib.testcontainers.docling.docling_ray import DoclingServeRayCluster

__all__ = ["DoclingServeContainer", "DoclingServeRayCluster"]
