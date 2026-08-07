# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""docling-serve testcontainer.

Provides a ``DoclingServeContainer`` that wraps the docling-serve Docker image
and exposes the base URL of the running server.

Usage::

    from tests.lib.testcontainers.docling import DoclingServeContainer

    container = DoclingServeContainer()
    container.start()

    url = container.get_url()   # e.g. "http://localhost:32768"
    container.stop()
"""

from tests.lib.testcontainers.docling.docling import DoclingServeContainer

__all__ = ["DoclingServeContainer"]
