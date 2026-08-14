"""Container test: S3-direct dispatch (source_ref) against a real, Ray-engine
docling-serve.

test_async_chain.py drives the parsing service's full submit/resolve/chunk
chain, but only via inline `file` uploads, against the default (local, non-Ray)
engine. test_ray_engine.py exercises the Ray engine's page-slice fan-out, but
bypasses the parsing service and MinIO entirely, going straight at
docling-serve via DoclingParseClient. Neither covers the thing that actually
needed a real upstream fix: submitting via `source_ref` routes through
`/v1/convert/source/batch` (the only docling-serve endpoint whose schema
accepts an S3 source), which is exactly the codepath docling-serve's
Ray-serde patch fixes (see tools/oci/images/docling) — and that codepath only
exists under the Ray engine. A test against the default engine would prove
nothing about the fix.

This drives the parsing service's `source_ref` submit path — the parsing
service never reads the file bytes, only verifies the object exists and
passes the bucket/key straight through — against the real, patched,
Ray-engine docling-serve + MinIO + the real backend-parsing app image.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import time
import uuid
from pathlib import Path

import httpx
import pytest
from minio import Minio

from src.lib.core.ingestion.types import ChunkType, ParseArtifact
from tests.lib.testcontainers.docling import DoclingServeRayClusterWithApp

_BAZEL_WORKSPACE = "_main"
_FIXTURE_DIR_REL = Path("tools/fixtures/docs")
_SOURCE_BUCKET = "ingest-source"
_CLUSTER_SERVICES = (
    "redis",
    "ray-head",
    "ray-worker",
    "docling-serve",
    "backend-parsing",
)


def _fixture(name: str) -> Path:
    """Locate a PDF fixture under tools/fixtures/docs/ (Bazel sandbox or direct
    pytest run from the repo root)."""
    srcdir = os.environ.get("TEST_SRCDIR")
    if srcdir:
        return Path(srcdir) / _BAZEL_WORKSPACE / _FIXTURE_DIR_REL / name
    return Path(__file__).parents[5] / _FIXTURE_DIR_REL / name


def _metadata(obj_id: uuid.UUID) -> str:
    return json.dumps({"obj_id": str(obj_id)})


def _read_artifact(minio_client: Minio, ref: dict) -> ParseArtifact:
    response = minio_client.get_object(ref["bucket"], ref["key"])
    try:
        return ParseArtifact.model_validate_json(response.read())
    finally:
        response.close()
        response.release_conn()


def _dump_cluster_logs(cluster: DoclingServeRayClusterWithApp) -> None:
    for service in _CLUSTER_SERVICES:
        try:
            stdout, stderr = cluster.get_logs(service)
        except Exception as exc:
            print(f"\n=== {service}: could not retrieve logs: {exc} ===")
            continue
        if stdout.strip() or stderr.strip():
            print(
                f"\n=== {service} stdout ===\n{stdout}"
                f"\n=== {service} stderr ===\n{stderr}"
            )


async def _poll_status(
    client: httpx.AsyncClient,
    task_id: str,
    timeout: float = 600.0,
    path: str = "/v1/parse/status",
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = await client.get(f"{path}/{task_id}")
        resp.raise_for_status()
        data = resp.json()
        if data["status"] == "success":
            return
        if data["status"] == "failure":
            raise RuntimeError(
                f"docling task {task_id!r} failed: {data.get('error_message')}"
            )
        await asyncio.sleep(5.0)
    raise TimeoutError(f"task {task_id!r} did not complete within {timeout}s")


async def _drive_async_chain_via_source_ref(
    client: httpx.AsyncClient,
    minio_client: Minio,
    pdf_bytes: bytes,
    key: str,
    obj_id: uuid.UUID,
) -> dict:
    """Seed the input in S3 first, then drive the full async chain via
    source_ref (S3-direct) — mirrors test_async_chain.py's
    _drive_async_chain, but via the S3-reference input mode instead of inline
    multipart."""
    if not minio_client.bucket_exists(_SOURCE_BUCKET):
        minio_client.make_bucket(_SOURCE_BUCKET)
    minio_client.put_object(_SOURCE_BUCKET, key, io.BytesIO(pdf_bytes), len(pdf_bytes))

    resp = await client.post(
        "/v1/parse/submit",
        data={
            "source_ref": json.dumps({"bucket": _SOURCE_BUCKET, "key": key}),
            "metadata": _metadata(obj_id),
        },
    )
    assert resp.status_code == 200, resp.text
    submit = resp.json()
    assert submit["mode"] == "source", (
        "submit_endpoint took the inline 'file' branch, not S3-direct "
        "'source' — this test would prove nothing about the Ray-serde fix"
    )

    await _poll_status(client, submit["parsing_task_id"])

    resp = await client.post("/v1/parse/resolve", json=submit)
    assert resp.status_code == 200, resp.text
    resolve = resp.json()

    resp = await client.post("/v1/chunk/submit", json={"obj_id": resolve["obj_id"]})
    assert resp.status_code == 200, resp.text
    chunk_submit = resp.json()

    await _poll_status(
        client, chunk_submit["chunking_task_id"], path="/v1/chunk/status"
    )

    resp = await client.post(
        "/v1/chunk/finalize",
        json={
            "chunking_task_id": chunk_submit["chunking_task_id"],
            "obj_id": chunk_submit["obj_id"],
            "metadata": {"obj_id": str(obj_id)},
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.fixture(scope="module")
def ray_app_cluster():
    with DoclingServeRayClusterWithApp() as cluster:
        yield cluster


@pytest.mark.integration
@pytest.mark.asyncio
async def test_source_ref_dispatches_s3_direct_against_real_docling_serve(
    ray_app_cluster: DoclingServeRayClusterWithApp,
) -> None:
    """The parsing service's source_ref submit path — S3 source + S3Target,
    /v1/convert/source/batch — completes correctly against a real, patched,
    Ray-engine docling-serve, and the resulting artifact is correctly written.
    """
    minio_client = ray_app_cluster.get_minio_client()
    pdf_bytes = _fixture("sample-tables.pdf").read_bytes()
    obj_id = uuid.uuid4()

    try:
        async with httpx.AsyncClient(
            base_url=ray_app_cluster.get_parsing_url(), timeout=300.0
        ) as client:
            ref = await _drive_async_chain_via_source_ref(
                client, minio_client, pdf_bytes, f"incoming/{obj_id}.pdf", obj_id
            )

        assert ref == {"bucket": "parsed-chunks", "key": f"parse/{obj_id}.json"}
        artifact = _read_artifact(minio_client, ref)
        assert len(artifact.chunks) >= 1
        assert all(c.obj_id == obj_id for c in artifact.chunks)
        assert len(artifact.pages) >= 1
        chunk_types = {c.type for c in artifact.chunks}
        assert ChunkType.TEXT in chunk_types
        assert ChunkType.TABLE in chunk_types

        # Scratch objects are cleaned up by resolve/finalize — nothing should
        # linger under the per-obj_id convert scratch prefix.
        scratch = list(
            minio_client.list_objects(
                "docling-replay", prefix=f"scratch/convert/{obj_id}/", recursive=True
            )
        )
        assert scratch == [], f"scratch objects not cleaned up: {scratch}"
    except Exception:
        _dump_cluster_logs(ray_app_cluster)
        raise
