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
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

OBJ_ID_A = uuid.uuid4()
OBJ_ID_B = uuid.uuid4()

# Pre-parsed chunks as would be produced by the parsing service.
_TEST_CHUNKS = [
    {
        "page_content": "This is a smoke test document with some content.",
        "source": "test.md",
        "type": "text",
        "obj_id": str(uuid.uuid4()),
    },
    {
        "page_content": "A second chunk to verify multi-chunk ingestion.",
        "source": "test.md",
        "type": "text",
        "obj_id": str(uuid.uuid4()),
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
        "/ingest",
        params={"namespace": str(namespace)},
        json=_TEST_CHUNKS,
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert body["num_added"] >= 1
    assert body["num_skipped"] == 0


# ---------------------------------------------------------------------------
# DELETE /ingest
# ---------------------------------------------------------------------------

_COLLECTION = "test_collection"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_missing_namespace_returns_422(client: httpx.AsyncClient):
    """DELETE /ingest without namespace → 422."""
    response = await client.delete("/ingest")
    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_nonexistent_obj_id_returns_204(client: httpx.AsyncClient):
    """DELETE /ingest for an obj_id that was never indexed → still 204 (idempotent)."""
    namespace = uuid.uuid4()
    response = await client.delete(
        "/ingest",
        params={"namespace": str(namespace), "obj_id": str(uuid.uuid4())},
    )
    assert response.status_code == 204


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_single_object_removes_only_target(
    client: httpx.AsyncClient,
    qdrant_client: AsyncQdrantClient,
):
    """Ingest two objects, delete one — only that object's chunks disappear."""
    namespace = uuid.uuid4()
    obj_id_a = uuid.uuid4()
    obj_id_b = uuid.uuid4()

    chunks_a = [
        {
            "page_content": "Source A content.",
            "source": "a.pdf",
            "type": "text",
            "obj_id": str(obj_id_a),
        }
    ]
    chunks_b = [
        {
            "page_content": "Source B content.",
            "source": "b.pdf",
            "type": "text",
            "obj_id": str(obj_id_b),
        }
    ]

    r = await client.post(
        "/ingest", json=chunks_a, params={"namespace": str(namespace)}
    )
    assert r.status_code == 200
    r = await client.post(
        "/ingest", json=chunks_b, params={"namespace": str(namespace)}
    )
    assert r.status_code == 200

    response = await client.delete(
        "/ingest",
        params={"namespace": str(namespace), "obj_id": str(obj_id_a)},
    )
    assert response.status_code == 204

    # obj_id_a chunks must be gone
    a_result = await qdrant_client.count(
        collection_name=_COLLECTION,
        count_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace", match=MatchValue(value=str(namespace))
                ),
                FieldCondition(
                    key="metadata.obj_id", match=MatchValue(value=str(obj_id_a))
                ),
            ]
        ),
    )
    assert a_result.count == 0

    # obj_id_b chunks must still be present
    b_result = await qdrant_client.count(
        collection_name=_COLLECTION,
        count_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace", match=MatchValue(value=str(namespace))
                ),
                FieldCondition(
                    key="metadata.obj_id", match=MatchValue(value=str(obj_id_b))
                ),
            ]
        ),
    )
    assert b_result.count > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_namespace_removes_all_chunks(
    client: httpx.AsyncClient,
    qdrant_client: AsyncQdrantClient,
):
    """Namespace deletion wipes all chunks and clears the record manager."""
    namespace = uuid.uuid4()

    r = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json=_TEST_CHUNKS,
    )
    assert r.status_code == 200
    assert r.json()["num_added"] >= 1

    response = await client.delete("/ingest", params={"namespace": str(namespace)})
    assert response.status_code == 204

    # Vectorstore must be empty for this namespace
    result = await qdrant_client.count(
        collection_name=_COLLECTION,
        count_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace", match=MatchValue(value=str(namespace))
                )
            ]
        ),
    )
    assert result.count == 0

    # Record manager must also be cleared — re-ingest must treat chunks as new
    r2 = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json=_TEST_CHUNKS,
    )
    assert r2.status_code == 200
    assert r2.json()["num_added"] >= 1
    assert r2.json()["num_skipped"] == 0


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
