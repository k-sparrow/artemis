"""Container smoke tests for the indexing service.

These tests start the real ``artemis/backend-indexing:dev`` image alongside
its infrastructure dependencies (Qdrant, TEI, Postgres) via testcontainers
and verify the service starts correctly and its HTTP API works end-to-end
against real infrastructure.

The indexing service no longer handles file uploads — it accepts
``List[ParsedChunk]`` JSON produced by the parsing service.

The ``retrieval_mode`` fixture (conftest) parametrizes the entire suite over
``dense`` and ``hybrid`` so every test runs twice — once per mode.  A
dedicated ``test_hybrid_sparse_vectors_stored`` test verifies the
BM25-specific behaviour that is only exercised in hybrid mode.
"""

import io
import json
import uuid

import httpx
import pytest
from minio import Minio
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

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
        json={"chunks": _TEST_CHUNKS},
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert body["num_added"] >= 1
    assert body["num_skipped"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_via_artifact_ref(client: httpx.AsyncClient, minio_client: Minio):
    """Claim-check: indexing reads chunks from a parse artifact in MinIO by reference."""
    namespace = uuid.uuid4()
    obj_id = uuid.uuid4()
    bucket, key = "parsed-chunks", f"parse/{obj_id}.json"
    artifact = {
        "pages": [
            {
                "obj_id": str(obj_id),
                "page_no": 1,
                "markdown": "# chunk read from the parse artifact",
            }
        ],
        "chunks": [
            {
                "page_content": "chunk read from the parse artifact",
                "source": "a.md",
                "type": "text",
                "obj_id": str(obj_id),
                "page_no": 1,
            }
        ],
    }
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
    data = json.dumps(artifact).encode()
    minio_client.put_object(
        bucket, key, io.BytesIO(data), length=len(data), content_type="application/json"
    )

    response = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"artifact_ref": {"bucket": bucket, "key": key}},
    )
    assert response.status_code == 200, response.text
    assert response.json()["num_added"] >= 1


# ---------------------------------------------------------------------------
# DELETE /ingest
# ---------------------------------------------------------------------------


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
    collection_name: str,
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
        "/ingest", json={"chunks": chunks_a}, params={"namespace": str(namespace)}
    )
    assert r.status_code == 200
    r = await client.post(
        "/ingest", json={"chunks": chunks_b}, params={"namespace": str(namespace)}
    )
    assert r.status_code == 200

    response = await client.delete(
        "/ingest",
        params={"namespace": str(namespace), "obj_id": str(obj_id_a)},
    )
    assert response.status_code == 204

    # obj_id_a chunks must be gone
    a_result = await qdrant_client.count(
        collection_name=collection_name,
        count_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace_id", match=MatchValue(value=str(namespace))
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
        collection_name=collection_name,
        count_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace_id", match=MatchValue(value=str(namespace))
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
    collection_name: str,
):
    """Namespace deletion wipes all chunks and clears the record manager."""
    namespace = uuid.uuid4()

    r = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"chunks": _TEST_CHUNKS},
    )
    assert r.status_code == 200
    assert r.json()["num_added"] >= 1

    response = await client.delete("/ingest", params={"namespace": str(namespace)})
    assert response.status_code == 204

    # Vectorstore must be empty for this namespace
    result = await qdrant_client.count(
        collection_name=collection_name,
        count_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace_id", match=MatchValue(value=str(namespace))
                )
            ]
        ),
    )
    assert result.count == 0

    # Record manager must also be cleared — re-ingest must treat chunks as new
    r2 = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"chunks": _TEST_CHUNKS},
    )
    assert r2.status_code == 200
    assert r2.json()["num_added"] >= 1
    assert r2.json()["num_skipped"] == 0


# ---------------------------------------------------------------------------
# Hybrid-mode specific assertions
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hybrid_sparse_vectors_stored(
    client: httpx.AsyncClient,
    qdrant_client: AsyncQdrantClient,
    collection_name: str,
    retrieval_mode: str,
):
    """In hybrid mode every stored point must carry a non-empty BM25 sparse vector.

    Skipped when running in dense mode — dense collections have no sparse vector
    field and the assertion would always fail there.
    """
    if retrieval_mode != "hybrid":
        pytest.skip("sparse vector check is only meaningful in hybrid mode")

    namespace = uuid.uuid4()
    r = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"chunks": _TEST_CHUNKS},
    )
    assert r.status_code == 200

    points, _ = await qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace_id", match=MatchValue(value=str(namespace))
                )
            ]
        ),
        with_vectors=True,
        limit=100,
    )

    assert len(points) >= 1
    for point in points:
        vectors = point.vector
        assert isinstance(
            vectors, dict
        ), f"Point {point.id}: expected named-vector dict, got {type(vectors)}"
        assert "sparse" in vectors, (
            f"Point {point.id} missing 'sparse' — "
            f"BM25 sparse vector was not stored in hybrid mode"
        )
        sparse = vectors["sparse"]
        assert (
            len(sparse.indices) > 0
        ), f"Point {point.id} has an empty sparse vector (no BM25 terms indexed)"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_colbert_vectors_stored(
    client: httpx.AsyncClient,
    qdrant_client: AsyncQdrantClient,
    collection_name: str,
    retrieval_mode: str,
):
    """In multi_stage mode every stored point must carry all three vector types:
    a named dense vector, a BM25 sparse vector, and a ColBERT multi-vector.

    Skipped in dense and hybrid modes — those collections have no ColBERT field.
    """
    if retrieval_mode != "multi_stage":
        pytest.skip("ColBERT vector check is only meaningful in multi_stage mode")

    namespace = uuid.uuid4()
    r = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"chunks": _TEST_CHUNKS},
    )
    assert r.status_code == 200

    points, _ = await qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace_id", match=MatchValue(value=str(namespace))
                )
            ]
        ),
        with_vectors=True,
        limit=100,
    )

    assert len(points) >= 1
    for point in points:
        vectors = point.vector
        assert isinstance(
            vectors, dict
        ), f"Point {point.id}: expected named-vector dict, got {type(vectors)}"
        assert "dense" in vectors, f"Point {point.id} missing 'dense' vector"
        assert "sparse" in vectors, f"Point {point.id} missing 'sparse' vector"
        assert "colbert" in vectors, f"Point {point.id} missing 'colbert' vector"
        colbert = vectors["colbert"]
        assert (
            isinstance(colbert, list) and len(colbert) > 0
        ), f"Point {point.id} has empty ColBERT multi-vector"
        assert all(
            len(tok) == 128 for tok in colbert
        ), f"Point {point.id}: ColBERT token vectors are not 128-dim"
