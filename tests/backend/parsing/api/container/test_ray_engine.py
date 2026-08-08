"""Container test for docling-serve's Ray engine (server-side PDF page-slice fan-out).

Exercises the Ray cluster fixture (redis + ray-head + ray-worker + docling-serve,
Epic 21) directly via ``DoclingParseClient`` — the same client the parsing service
uses — bypassing the parsing service and MinIO entirely. This tests the
infrastructure/engine capability itself: does docling-serve's Ray engine convert a
multi-page PDF correctly when configured to fan pages out across Ray actors. The
parsing service's own submit/resolve/finalize request handling is already covered
by test_async_chain.py against the local engine — no need to re-test that mapping
logic here (see CLAUDE.md: don't test the same thing at two layers).

Uses the existing 50-page fixture (no >400-page fixture is checked into the repo —
adding one is a separate follow-up, not done here to avoid bloating the repo with a
large binary). docker-compose.ray.yaml pins DOCLING_SERVE_ENG_RAY_MAX_PAGE_SLICE_SIZE
to 10, so the 50-page fixture still forces 5 page slices — real fan-out, not a
single-slice no-op.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from src.backend.parsing.lib.artifact import ParseStatus
from src.backend.parsing.lib.docling_client import DoclingParseClient
from tests.lib.testcontainers.docling import DoclingServeRayCluster

# Bazel bzlmod uses "_main" as the canonical workspace name for the main module,
# not the module name declared in MODULE.bazel.
_BAZEL_WORKSPACE = "_main"
_FIXTURE_DIR_REL = Path("tools/fixtures/docs")
_FIXTURE_PAGE_COUNT = 50
_CLUSTER_SERVICES = ("redis", "ray-head", "ray-worker", "docling-serve")


def _dump_cluster_logs(cluster: DoclingServeRayCluster) -> None:
    """Print every service's logs — the compose stack is torn down as soon as
    the test exits, so this is the only chance to see why docling-serve
    reported a failure (its own error messages are often as opaque as
    "Internal processing error", with the real cause only in its own logs)."""
    for service in _CLUSTER_SERVICES:
        try:
            stdout, stderr = cluster.get_logs(service)
        except Exception as exc:
            print(f"\n=== {service}: could not retrieve logs: {exc} ===")
            continue
        if stdout.strip() or stderr.strip():
            print(
                f"\n=== {service} stdout ===\n{stdout}\n=== {service} stderr ===\n{stderr}"
            )


def _fixture(name: str) -> Path:
    """Locate a PDF fixture under tools/fixtures/docs/.

    Works both in the Bazel sandbox (via TEST_SRCDIR) and in direct pytest
    runs from the repo root.
    """
    srcdir = os.environ.get("TEST_SRCDIR")
    if srcdir:
        return Path(srcdir) / _BAZEL_WORKSPACE / _FIXTURE_DIR_REL / name
    return Path(__file__).parents[5] / _FIXTURE_DIR_REL / name


async def _poll_status(
    client: DoclingParseClient, task_id: str, *, timeout: float = 600.0
) -> ParseStatus:
    """Poll get_status until terminal; raise on failure or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = await client.get_status(task_id, timeout=30.0)
        if status.status == "success":
            return status
        if status.status == "failure":
            raise RuntimeError(
                f"docling task {task_id!r} failed: {status.error_message}"
            )
        await asyncio.sleep(5.0)
    raise TimeoutError(f"task {task_id!r} did not complete within {timeout}s")


@pytest.fixture(scope="module")
def ray_cluster() -> Iterator[DoclingServeRayCluster]:
    with DoclingServeRayCluster() as cluster:
        yield cluster


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ray_engine_converts_multi_slice_pdf(
    ray_cluster: DoclingServeRayCluster,
) -> None:
    """A whole multi-page PDF, submitted as a single file, converts correctly
    through the Ray engine — Artemis's client only ever sees one task_id and a
    single submit/poll/fetch round trip, identical to a small document, while
    the Ray cluster fans the pages out into slices and converts them
    concurrently across actors server-side (Epic 21).
    """
    client = DoclingParseClient(ray_cluster.get_url())
    pdf_bytes = _fixture("sample-50-page-pdf-a4-size.pdf").read_bytes()

    try:
        task_id = await client.submit_file(
            content=pdf_bytes,
            filename="sample-50-page.pdf",
            content_type="application/pdf",
            timeout=120.0,
        )

        status = await _poll_status(client, task_id, timeout=600.0)
        # docling-serve's task_meta population under Ray page-slice fan-out for a
        # single-file submission isn't a documented contract — assert on it only
        # when present, never require it, so this doesn't become a flaky test tied
        # to an internal detail.
        if status.num_total is not None:
            assert status.num_total >= 1

        dl_doc = await client.fetch_conversion_result(task_id, timeout=60.0)
        assert len(dl_doc.pages) == _FIXTURE_PAGE_COUNT, (
            f"expected all {_FIXTURE_PAGE_COUNT} pages reassembled from Ray fan-out, "
            f"got {len(dl_doc.pages)}"
        )
    except Exception:
        _dump_cluster_logs(ray_cluster)
        raise
