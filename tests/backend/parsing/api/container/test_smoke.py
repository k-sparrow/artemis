"""Container smoke tests for the parsing service.

These tests start the real ``artemis/backend-parsing:dev`` image alongside
Docling + MinIO testcontainers and verify the service comes up healthy.
Parse-flow coverage against the real containers lives in
``test_async_chain.py`` (the submit/status/resolve/chunk chain).
"""

import httpx
import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_liveness_returns_200(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/liveness")
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readiness_returns_200(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/readiness")
    assert response.status_code == 200, response.json()
