"""Subsystem e2e test: full enterprise intake pipeline.

Stack (all session-scoped, wired in conftest.py):

  POST /data-sources              [data_sources control plane]
      → KafkaConnect deploys FileSource connector
      → FileSource (Camel, /watch)
      → artemis.datasource.filesystem
      → ksqlDB (header extraction → IntakeRequest JSON)
      → artemis.datasource.filesystem.intake
      → HTTP sink (deployed by intake service lifespan)
      → POST /intake  [real intake service container]
      → GET  /namespaces/{id}          [WireMock — storage stub]
      → POST /namespaces/{id}/objects  [WireMock — storage stub]

Tests write uniquely-named files into ``session_watch_dir`` (mounted at /watch
in both KafkaConnect and the intake container) and assert that WireMock
received a multipart upload POST to the storage service — proving the full
pipeline ran end-to-end.

Pre-requisites:
    bazel run //src/backend/enterprise/data_sources/api:tarball.dev
    bazel run //src/backend/enterprise/intake/api:tarball.dev
    (artemis/cp-kafka-connect:latest must also be built)
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import httpx
import pytest

_NAMESPACE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wait_for_storage_upload(
    wiremock_host_url: str,
    namespace_id: str = _NAMESPACE_ID,
    count: int = 1,
    timeout: int = 90,
) -> list[dict]:
    """Poll WireMock until at least ``count`` POSTs to /namespaces/{id}/objects appear."""
    expected_path = f"/namespaces/{namespace_id}/objects"
    deadline = time.monotonic() + timeout
    calls: list[dict] = []
    while time.monotonic() < deadline:
        resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
        calls = [
            r["request"]
            for r in resp.json().get("requests", [])
            if r["request"]["method"] == "POST" and r["request"]["url"] == expected_path
        ]
        if len(calls) >= count:
            return calls
        time.sleep(2)
    pytest.fail(
        f"WireMock received only {len(calls)} POST {expected_path} within {timeout}s. "
        f"All recorded: {httpx.get(f'{wiremock_host_url}/__admin/requests').json()}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """End-to-end: file drop → FileSource → ksqlDB → HTTP sink → intake → storage.

    The ``data_source`` fixture creates a FileSource connector via the
    data_sources control plane and waits for RUNNING before any test runs.
    Files are written with unique names so the idempotent FileSource connector
    never suppresses redelivery from previous tests.
    """

    @pytest.fixture(autouse=True)
    def _require_pipeline(self, data_source: dict) -> None:
        """Pull in the full session-scoped pipeline."""

    def test_file_triggers_storage_upload(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
    ) -> None:
        """A single file dropped into /watch reaches the storage service as an upload."""
        fname = f"smoke-{uuid.uuid4().hex[:8]}.txt"
        (session_watch_dir / fname).write_text("test content")

        calls = _wait_for_storage_upload(wiremock_host_url, count=1)
        assert len(calls) >= 1

    def test_two_files_trigger_two_storage_uploads(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
    ) -> None:
        """Two files in /watch produce two independent storage upload POSTs."""
        fnames = [
            f"alpha-{uuid.uuid4().hex[:8]}.txt",
            f"beta-{uuid.uuid4().hex[:8]}.txt",
        ]
        for fname in fnames:
            (session_watch_dir / fname).write_text(f"content of {fname}")

        calls = _wait_for_storage_upload(wiremock_host_url, count=2, timeout=120)
        assert len(calls) >= 2
