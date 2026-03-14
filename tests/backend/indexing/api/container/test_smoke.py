"""Container smoke tests for the indexing service.

These tests start the real ``artemis/backend-indexing:dev`` image alongside
its infrastructure dependencies (Qdrant, TEI, Postgres, Docling) via
testcontainers and verify the service starts correctly and its HTTP API works
end-to-end against real infrastructure.
"""

import uuid

import httpx
import pytest

# Minimal Markdown document — Docling supports .md natively.
_TEST_DOCUMENT = b"# Test Document\n\nThis is a smoke test document with some content.\n"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_liveness_returns_200(client: httpx.AsyncClient):
    response = await client.get("/health/liveness")
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readiness_returns_200(client: httpx.AsyncClient):
    response = await client.get("/health/readiness")
    assert response.status_code == 200, response.json()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_indexes_document(client: httpx.AsyncClient):
    """End-to-end ingest: Docling parses the document, TEI embeds it, Qdrant stores it."""
    namespace = uuid.uuid4()
    response = await client.post(
        f"/ingest?namespace={namespace}",
        files={"file": ("test.md", _TEST_DOCUMENT, "text/markdown")},
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert body["num_added"] >= 1
    assert body["num_skipped"] == 0
