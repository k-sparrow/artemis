# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Session-scoped Docling testcontainer for DoclingAPIServeLoader integration tests."""

from __future__ import annotations

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

_DOCLING_IMAGE = "quay.io/docling-project/docling-serve:latest"  # CPU variant
_DOCLING_PORT = 5001


@pytest.fixture(scope="session")
def docling_container(request: pytest.FixtureRequest) -> DockerContainer:
    container = (
        DockerContainer(_DOCLING_IMAGE)
        .with_exposed_ports(_DOCLING_PORT)
        .waiting_for(HttpWaitStrategy(_DOCLING_PORT, "/health").for_status_code(200))
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def docling_base_url(docling_container: DockerContainer) -> str:
    host = docling_container.get_container_host_ip()
    port = int(docling_container.get_exposed_port(_DOCLING_PORT))
    return f"http://{host}:{port}"
