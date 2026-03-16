"""Container smoke tests for the indexing service.

These tests start the real ``artemis/backend-indexing:dev`` image alongside
its infrastructure dependencies (Qdrant, TEI, Postgres) via testcontainers
and verify the service starts correctly and its HTTP API works end-to-end
against real infrastructure.

The indexing service no longer handles file uploads — it accepts
``List[ParsedChunk]`` JSON produced by the parsing service.
"""

import uuid

import httpx
import pytest

# Pre-parsed chunks as would be produced by the parsing service.
_TEST_CHUNKS = [
    {
        "page_content": "This is a smoke test document with some content.",
        "source": "test.md",
        "type": "text",
    },
    {
        "page_content": "A second chunk to verify multi-chunk ingestion.",
        "source": "test.md",
        "type": "text",
    },
]


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
async def test_ingest_indexes_chunks(client: httpx.AsyncClient):
    """End-to-end ingest: pre-parsed chunks are embedded by TEI and stored in Qdrant."""
    namespace = uuid.uuid4()
    response = await client.post(
        f"/ingest?namespace={namespace}",
        json=_TEST_CHUNKS,
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert body["num_added"] >= 1
    assert body["num_skipped"] == 0


# ---------------------------------------------------------------------------
# TODO: extend this suite (Epic 6 / post-Epic 6)
# ---------------------------------------------------------------------------

# TODO: test idempotency — ingest the same document twice; assert first run
#   produces num_added=N / num_skipped=0, second run produces num_added=0 /
#   num_skipped=N.  Exercises the record-manager deduplication path end-to-end.

# TODO: test retrieval — after a successful ingest, query Qdrant directly (or
#   via a retrieval endpoint) with a vector in the same namespace and assert at
#   least one result is returned with the correct namespace in metadata.
#   Catches corrupt vectors and wrong namespace stamping.

# TODO: test a TABLE chunk type — send a chunk with type="table" and verify
#   it is stored correctly (metadata.type == "table" in Qdrant payload).

# TODO: test error handling / robustness:
#   - Empty chunks list → expect 200 with num_added=0
#   - Qdrant unavailable at request time → expect 503
#   - TEI unavailable at request time → expect 503
#   - Malformed namespace query param → expect 422
#   - Missing required field in chunk (e.g. no page_content) → expect 422
