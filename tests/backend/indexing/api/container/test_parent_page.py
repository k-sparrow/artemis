"""Container integration tests for parent-page indexing & retrieval (Epic 19).

Round-trips the real ``artemis/backend-indexing:dev`` image against MinIO,
Qdrant and TEI (via the ``container`` conftest) and asserts the parent-page
*layer* end to end:

  - pages are written to the ``PAGE_BUCKET`` doc store at **deterministic keys**
    (``{namespace}/{obj_id}/p{page_no}``);
  - every chunk in Qdrant carries a ``parent_id`` pointing at its page key;
  - ``/retrieve`` with ``return_parents`` dereferences the matched chunks back to
    their parent pages (N chunks → their distinct pages);
  - re-ingest is a free no-op (pages + chunks skipped, no re-embed);
  - re-ingest with fewer pages prefix-trims the now-stale page from the store;
  - per-object and per-namespace deletion prefix-remove the cached pages.

These exercise the parent-page decorator over the service's default (Simple)
pipeline.  The algorithm-agnostic composition guarantee — that the SAME
``ParentPagePipeline`` wraps Simple *and* SemiStructured without either algorithm
knowing about pages — is proven structurally at the unit layer
(``tests/lib/core/ingestion/pipeline/test_parent_page.py::TestComposability``).
A SemiStructured *container* run is intentionally deferred: it needs an LLM
table-summarizer that the indexing service's SemiStructured resources do not yet
wire, so such a test would exercise the summarizer infra, not this layer.

The ``retrieval_mode`` fixture (conftest) parametrizes the whole suite over
``dense`` / ``hybrid`` / ``multi_stage`` — parent-page deref is mode-agnostic, so
every test runs once per mode.
"""

import io
import json
import uuid
from typing import Any

import httpx
import pytest
from minio import Minio
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

# The parent-page doc store bucket — created by the app lifespan's ensure_bucket
# (see conftest ``PAGE_BUCKET`` env); the same MinIO the host client inspects.
_PAGE_BUCKET = "parent-pages"
# Where the seeded parse artifacts live (matches the smoke-test convention).
_ARTIFACT_BUCKET = "parsed-chunks"


def _page_key(namespace: uuid.UUID, obj_id: uuid.UUID, page_no: int) -> str:
    """Independent oracle of the store-key contract.

    Deliberately NOT imported from ``parent_page.page_key``: a drifted
    implementation must *fail* this test, not silently agree with it.
    """
    return f"{namespace}/{obj_id}/p{page_no}"


def _make_doc(obj_id: uuid.UUID) -> tuple[list[dict], list[dict]]:
    """A 2-page document with 3 chunks (2 on page 1, 1 on page 2).

    The 2→1 page fan-in lets the retrieval test assert parent dedup.
    """
    pages = [
        {
            "obj_id": str(obj_id),
            "page_no": 1,
            "markdown": "# Page One\nQuarterly revenue grew across all regions.",
        },
        {
            "obj_id": str(obj_id),
            "page_no": 2,
            "markdown": "# Page Two\nOperating costs were reduced via automation.",
        },
    ]
    chunks = [
        {
            "page_content": "Quarterly revenue grew across all regions.",
            "source": "report.pdf",
            "type": "text",
            "obj_id": str(obj_id),
            "page_no": 1,
        },
        {
            "page_content": "Revenue growth was strongest in the EMEA segment.",
            "source": "report.pdf",
            "type": "text",
            "obj_id": str(obj_id),
            "page_no": 1,
        },
        {
            "page_content": "Operating costs were reduced via automation.",
            "source": "report.pdf",
            "type": "text",
            "obj_id": str(obj_id),
            "page_no": 2,
        },
    ]
    return pages, chunks


def _seed_artifact(
    minio_client: Minio,
    *,
    namespace: uuid.UUID,
    obj_id: uuid.UUID,
    pages: list[dict],
    chunks: list[dict],
) -> dict[str, str]:
    """Write a ``ParseArtifact`` to MinIO and return its ``BlobRef`` dict."""
    key = f"parse/{namespace}/{obj_id}.json"
    data = json.dumps({"pages": pages, "chunks": chunks}).encode()
    if not minio_client.bucket_exists(_ARTIFACT_BUCKET):
        minio_client.make_bucket(_ARTIFACT_BUCKET)
    minio_client.put_object(
        _ARTIFACT_BUCKET,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    return {"bucket": _ARTIFACT_BUCKET, "key": key}


async def _ingest(
    client: httpx.AsyncClient, namespace: uuid.UUID, ref: dict[str, str]
) -> dict[str, Any]:
    r = await client.post(
        "/ingest",
        params={"namespace": str(namespace)},
        json={"artifact_ref": ref},
    )
    assert r.status_code == 200, r.text
    return r.json()


def _page_keys_under(minio_client: Minio, prefix: str) -> list[str]:
    return [
        obj.object_name
        for obj in minio_client.list_objects(
            _PAGE_BUCKET, prefix=prefix, recursive=True
        )
    ]


def _namespace_filter(namespace: uuid.UUID, obj_id: uuid.UUID | None = None) -> Filter:
    must = [
        FieldCondition(
            key="metadata.namespace_id", match=MatchValue(value=str(namespace))
        )
    ]
    if obj_id is not None:
        must.append(
            FieldCondition(key="metadata.obj_id", match=MatchValue(value=str(obj_id)))
        )
    return Filter(must=must)


# ---------------------------------------------------------------------------
# Index side — pages written, chunks linked
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pages_written_to_doc_store_at_deterministic_keys(
    client: httpx.AsyncClient, minio_client: Minio
):
    """Ingest caches each page in the PAGE_BUCKET at ``{ns}/{obj}/p{n}``."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )

    await _ingest(client, namespace, ref)

    for page_no in (1, 2):
        key = _page_key(namespace, obj_id, page_no)
        stat = minio_client.stat_object(_PAGE_BUCKET, key)  # raises if absent
        assert stat.size > 0, f"page {key} cached but empty"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_chunks_carry_parent_id_pointing_at_their_page(
    client: httpx.AsyncClient,
    minio_client: Minio,
    qdrant_client: AsyncQdrantClient,
    collection_name: str,
):
    """Every stored chunk carries ``parent_id == page_key(ns, obj, its page_no)``."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )

    await _ingest(client, namespace, ref)

    points, _ = await qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=_namespace_filter(namespace),
        with_payload=True,
        limit=100,
    )
    assert len(points) == len(chunks)
    for point in points:
        md = point.payload["metadata"]
        assert md["parent_id"] == _page_key(
            namespace, obj_id, md["page_no"]
        ), f"chunk on page {md['page_no']} has parent_id {md['parent_id']!r}"
        assert md["parent_kind"] == "page"


# ---------------------------------------------------------------------------
# Retrieve side — per-request parent-page dereference
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retrieve_return_parents_derefs_chunks_to_pages(
    client: httpx.AsyncClient, minio_client: Minio
):
    """``return_parents`` returns the parent pages, deduped N chunks → pages."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )
    await _ingest(client, namespace, ref)

    r = await client.post(
        "/retrieve/invoke",
        json={
            "input": "revenue and costs",
            "config": {
                "configurable": {
                    "namespace_id": str(namespace),
                    "return_parents": True,
                    "k": 10,
                }
            },
        },
    )
    assert r.status_code == 200, r.text
    docs = r.json()["output"]

    # 3 chunks across 2 pages → exactly the 2 distinct parent pages, no chunks.
    contents = {d["page_content"] for d in docs}
    assert contents == {pages[0]["markdown"], pages[1]["markdown"]}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retrieve_without_flag_returns_chunks(
    client: httpx.AsyncClient, minio_client: Minio
):
    """Control: without ``return_parents`` the response is the chunks themselves
    (still carrying ``parent_id``) — the deref is strictly opt-in."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )
    await _ingest(client, namespace, ref)

    r = await client.post(
        "/retrieve/invoke",
        json={
            "input": "revenue and costs",
            "config": {"configurable": {"namespace_id": str(namespace), "k": 10}},
        },
    )
    assert r.status_code == 200, r.text
    docs = r.json()["output"]

    chunk_contents = {c["page_content"] for c in chunks}
    returned = {d["page_content"] for d in docs}
    assert returned <= chunk_contents and returned, "expected chunk content, not pages"
    for d in docs:
        assert d["metadata"]["parent_id"] == _page_key(
            namespace, obj_id, d["metadata"]["page_no"]
        )


# ---------------------------------------------------------------------------
# Re-ingest — free diffing, shrink reconcile
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reingest_is_a_free_noop(client: httpx.AsyncClient, minio_client: Minio):
    """Re-ingesting identical content skips every chunk (no re-embed)."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )

    first = await _ingest(client, namespace, ref)
    assert first["num_added"] == len(chunks)

    second = await _ingest(client, namespace, ref)
    assert second["num_added"] == 0
    assert second["num_skipped"] == len(chunks)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reingest_with_fewer_pages_trims_stale_page(
    client: httpx.AsyncClient, minio_client: Minio
):
    """Dropping a page on re-ingest prefix-trims its now-stale store key (§9
    reconcile) while leaving the surviving page in place."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )
    await _ingest(client, namespace, ref)

    # Re-ingest with only page 1 (and only its chunks).
    fewer_pages = [pages[0]]
    fewer_chunks = [c for c in chunks if c["page_no"] == 1]
    ref2 = _seed_artifact(
        minio_client,
        namespace=namespace,
        obj_id=obj_id,
        pages=fewer_pages,
        chunks=fewer_chunks,
    )
    await _ingest(client, namespace, ref2)

    remaining = set(_page_keys_under(minio_client, f"{namespace}/{obj_id}/"))
    assert _page_key(namespace, obj_id, 1) in remaining
    assert _page_key(namespace, obj_id, 2) not in remaining


# ---------------------------------------------------------------------------
# Delete — pages prefix-removed alongside chunks
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_object_removes_its_pages(
    client: httpx.AsyncClient,
    minio_client: Minio,
    qdrant_client: AsyncQdrantClient,
    collection_name: str,
):
    """Per-object delete prefix-removes the object's pages and its chunks."""
    namespace, obj_id = uuid.uuid4(), uuid.uuid4()
    pages, chunks = _make_doc(obj_id)
    ref = _seed_artifact(
        minio_client, namespace=namespace, obj_id=obj_id, pages=pages, chunks=chunks
    )
    await _ingest(client, namespace, ref)
    assert _page_keys_under(minio_client, f"{namespace}/{obj_id}/")  # present first

    r = await client.delete(
        "/ingest", params={"namespace": str(namespace), "obj_id": str(obj_id)}
    )
    assert r.status_code == 204

    assert _page_keys_under(minio_client, f"{namespace}/{obj_id}/") == []
    count = await qdrant_client.count(
        collection_name=collection_name,
        count_filter=_namespace_filter(namespace, obj_id),
    )
    assert count.count == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_namespace_removes_all_pages(
    client: httpx.AsyncClient, minio_client: Minio
):
    """Namespace wipe prefix-removes every page under ``{namespace}/``."""
    namespace = uuid.uuid4()
    obj_a, obj_b = uuid.uuid4(), uuid.uuid4()
    for obj_id in (obj_a, obj_b):
        pages, chunks = _make_doc(obj_id)
        ref = _seed_artifact(
            minio_client,
            namespace=namespace,
            obj_id=obj_id,
            pages=pages,
            chunks=chunks,
        )
        await _ingest(client, namespace, ref)
    assert _page_keys_under(minio_client, f"{namespace}/")  # both objects present

    r = await client.delete("/ingest", params={"namespace": str(namespace)})
    assert r.status_code == 204

    assert _page_keys_under(minio_client, f"{namespace}/") == []


# ---------------------------------------------------------------------------
# Reproduction: accidental namespace wipe via degraded (empty) chunk batch
# ---------------------------------------------------------------------------
#
# Unlike test_delete_namespace_removes_all_pages above (a deliberate wipe via
# DELETE /ingest), this reproduces an ACCIDENTAL one via an ordinary POST
# /ingest whose chunk step came back empty for reasons unrelated to any other
# object in the namespace (e.g. a docling-serve chunking failure that
# succeeded at HTTP level but produced zero chunks).
#
# PagedInput.is_empty() (parent_page.py) only treats a call as an intentional
# wipe when BOTH pages and chunks are empty. If pages is non-empty but
# chunks is empty, the pipeline proceeds normally and delegates chunks=[] to
# the inner upserter's aupsert([]), which calls LangChain's
# aindex(docs_source=[], cleanup="scoped_full", source_id_key="obj_id").
# Because there is nothing to iterate, the source-id set LangChain uses to
# scope cleanup stays empty (not "contains this object's obj_id" — literally
# empty), which becomes group_ids=[] in SQLRecordManager.list_keys/alist_keys.
# `if group_ids:` treats an empty list as falsy and skips the group filter
# entirely — so cleanup lists (and deletes) every key in the WHOLE namespace
# predating the call, not just the empty-chunk object's own (nonexistent) keys.


@pytest.mark.integration
@pytest.mark.asyncio
async def test_empty_chunk_batch_does_not_wipe_other_objects_in_namespace(
    client: httpx.AsyncClient,
    minio_client: Minio,
    qdrant_client: AsyncQdrantClient,
    collection_name: str,
):
    """A second object's degraded (zero-chunk) ingest must not delete a
    first, unrelated object's already-indexed vectors in the same namespace."""
    namespace = uuid.uuid4()
    obj_a, obj_b = uuid.uuid4(), uuid.uuid4()

    # Object A: ingested normally and successfully first.
    pages_a, chunks_a = _make_doc(obj_a)
    ref_a = _seed_artifact(
        minio_client,
        namespace=namespace,
        obj_id=obj_a,
        pages=pages_a,
        chunks=chunks_a,
    )
    await _ingest(client, namespace, ref_a)

    points_before, _ = await qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=_namespace_filter(namespace, obj_a),
        with_payload=False,
        limit=100,
    )
    assert len(points_before) == len(chunks_a), "sanity: object A indexed correctly"

    # Object B: pages parsed fine, but chunking degraded to zero — the exact
    # condition PagedInput.is_empty() does not catch.
    pages_b = [
        {
            "obj_id": str(obj_b),
            "page_no": 1,
            "markdown": "# Some Page\nContent that failed to chunk.",
        }
    ]
    ref_b = _seed_artifact(
        minio_client,
        namespace=namespace,
        obj_id=obj_b,
        pages=pages_b,
        chunks=[],
    )
    await _ingest(client, namespace, ref_b)

    points_after, _ = await qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=_namespace_filter(namespace, obj_a),
        with_payload=False,
        limit=100,
    )
    assert len(points_after) == len(chunks_a), (
        f"object A's vectors were wiped by object B's empty-chunk ingest: "
        f"had {len(points_before)}, now {len(points_after)}"
    )
