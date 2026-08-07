"""Container integration tests for the async parse chain endpoints.

Drives the full submit → status → resolve → finalize chain against the real
``artemis/backend-parsing:dev`` image backed by docling-serve and MinIO
testcontainers, using real PDF fixtures from ``tools/fixtures/docs/``.

Large-PDF fan-out is no longer a client-side concern (see TODOs.md Epic 21) —
every document takes this same submit/resolve path regardless of size, with
docling-serve's Ray engine handling server-side page-slice fan-out for large
documents. A Ray-cluster container test covering that fan-out path is tracked
separately (Epic 21 §21.5), pending the Ray-backed testcontainer fixture.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path

import httpx
import pytest
from minio import Minio

from src.lib.core.ingestion.types import ChunkType, ParseArtifact

# Bazel bzlmod uses "_main" as the canonical workspace name for the main module,
# not the module name declared in MODULE.bazel.
_BAZEL_WORKSPACE = "_main"
_FIXTURE_DIR_REL = Path("tools/fixtures/docs")


def _fixture(name: str) -> Path:
    """Locate a PDF fixture under tools/fixtures/docs/.

    Works both in the Bazel sandbox (via TEST_SRCDIR) and in direct pytest
    runs from the repo root.
    """
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


async def _poll_status(
    client: httpx.AsyncClient, task_id: str, timeout: float = 300.0
) -> None:
    """Poll GET /v1/parse/status/{task_id} until terminal; raise on failure."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = await client.get(f"/v1/parse/status/{task_id}")
        resp.raise_for_status()
        data = resp.json()
        if data["status"] == "success":
            return
        if data["status"] == "failure":
            raise RuntimeError(
                f"Docling task {task_id!r} failed: {data.get('error_message')}"
            )
        await asyncio.sleep(5.0)
    raise TimeoutError(f"Task {task_id!r} did not complete within {timeout}s")


async def _drive_async_chain(
    client: httpx.AsyncClient,
    pdf_bytes: bytes,
    filename: str,
    obj_id: uuid.UUID,
) -> dict:
    """Drive the full async parse chain; return the finalize BlobRef dict."""
    resp = await client.post(
        "/v1/parse/submit",
        files={"file": (filename, pdf_bytes, "application/pdf")},
        data={"metadata": _metadata(obj_id)},
    )
    assert resp.status_code == 200, resp.text
    submit = resp.json()

    await _poll_status(client, submit["parsing_task_id"])

    resp = await client.post(
        "/v1/parse/resolve",
        json={
            "parsing_task_id": submit["parsing_task_id"],
            "obj_id": submit["obj_id"],
        },
    )
    assert resp.status_code == 200, resp.text
    resolve = resp.json()

    await _poll_status(client, resolve["chunking_task_id"])

    resp = await client.post(
        "/v1/parse/finalize",
        json={
            "chunking_task_id": resolve["chunking_task_id"],
            "obj_id": resolve["obj_id"],
            "metadata": {"obj_id": str(obj_id)},
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_chain_text_pdf_produces_artifact(
    client: httpx.AsyncClient, minio_client: Minio
) -> None:
    """Full async chain with a text-only PDF: chunks, pages, and replay cache
    must all be written to MinIO under the correct keys."""
    pdf_bytes = _fixture("sample-text-only-pdf-a4-size.pdf").read_bytes()
    obj_id = uuid.uuid4()

    ref = await _drive_async_chain(client, pdf_bytes, "sample-text.pdf", obj_id)

    assert ref == {"bucket": "parsed-chunks", "key": f"parse/{obj_id}.json"}
    artifact = _read_artifact(minio_client, ref)
    assert len(artifact.chunks) >= 1
    assert all(c.obj_id == obj_id for c in artifact.chunks)
    assert len(artifact.pages) >= 1

    # Replay cache must survive finalize (it is written by resolve, not deleted).
    minio_client.stat_object("docling-replay", f"replay/{obj_id}.json")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_chain_tables_pdf_produces_text_and_table_chunks(
    client: httpx.AsyncClient, minio_client: Minio
) -> None:
    """PDF with tables: the artifact must contain both TEXT and TABLE chunk types."""
    pdf_bytes = _fixture("sample-tables.pdf").read_bytes()
    obj_id = uuid.uuid4()

    ref = await _drive_async_chain(client, pdf_bytes, "sample-tables.pdf", obj_id)

    chunk_types = {c.type for c in _read_artifact(minio_client, ref).chunks}
    assert ChunkType.TEXT in chunk_types, "Expected at least one TEXT chunk"
    assert ChunkType.TABLE in chunk_types, "Expected at least one TABLE chunk"
