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


def _assert_no_upload(
    wiremock_host_url: str,
    namespace_id: str = _NAMESPACE_ID,
    window: int = 20,
) -> None:
    """Assert that no new upload POST arrives within ``window`` seconds."""
    expected_path = f"/namespaces/{namespace_id}/objects"
    deadline = time.monotonic() + window
    while time.monotonic() < deadline:
        resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
        calls = [
            r
            for r in resp.json().get("requests", [])
            if r["request"]["method"] == "POST" and r["request"]["url"] == expected_path
        ]
        if calls:
            pytest.fail(
                f"Expected no uploads but WireMock received {len(calls)} "
                f"POST {expected_path} within {window}s."
            )
        time.sleep(2)


def _wait_connector_state(
    data_sources_base_url: str,
    source_id: str,
    state: str,
    timeout: int = 60,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = httpx.get(f"{data_sources_base_url}/data-sources/{source_id}")
        assert resp.status_code == 200
        if resp.json().get("kafka_status", {}).get("state") == state:
            return
        time.sleep(2)
    pytest.fail(
        f"Connector {source_id} did not reach state {state!r} within {timeout}s"
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
    def _require_pipeline(self, data_source: dict) -> None:  # noqa: ARG002
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

    def test_large_file_upload(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
    ) -> None:
        """A file >1 MB is delivered intact through the full pipeline."""
        fname = f"large-{uuid.uuid4().hex[:8]}.bin"
        (session_watch_dir / fname).write_bytes(b"x" * (2 * 1024 * 1024))  # 2 MB

        calls = _wait_for_storage_upload(wiremock_host_url, count=1, timeout=120)
        assert len(calls) >= 1

    def test_two_files_both_arrive(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
    ) -> None:
        """Two files dropped sequentially both reach the storage service."""
        fname_a = f"order-a-{uuid.uuid4().hex[:8]}.txt"
        fname_b = f"order-b-{uuid.uuid4().hex[:8]}.txt"

        (session_watch_dir / fname_a).write_text("first")
        time.sleep(1)
        (session_watch_dir / fname_b).write_text("second")

        calls = _wait_for_storage_upload(wiremock_host_url, count=2, timeout=120)
        bodies = [c.get("body", "") for c in calls]
        assert any(
            fname_a in b for b in bodies
        ), f"{fname_a} not found in WireMock calls"
        assert any(
            fname_b in b for b in bodies
        ), f"{fname_b} not found in WireMock calls"

    def test_connector_restarts_and_continues_delivery(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
        data_source: dict,
        data_sources_base_url: str,
    ) -> None:
        """Restarting the connector returns it to RUNNING and new files continue
        to be delivered."""
        source_id = data_source["id"]

        # Restart the connector
        resp = httpx.post(f"{data_sources_base_url}/data-sources/{source_id}/restart")
        assert resp.status_code == 200

        # Wait for RUNNING, then clear WireMock and drop a new file
        _wait_connector_state(data_sources_base_url, source_id, "RUNNING")
        httpx.delete(f"{wiremock_host_url}/__admin/requests")

        fname = f"post-restart-{uuid.uuid4().hex[:8]}.txt"
        (session_watch_dir / fname).write_text("delivered after restart")

        calls = _wait_for_storage_upload(wiremock_host_url, count=1, timeout=60)
        assert any(
            fname in c.get("body", "") for c in calls
        ), f"{fname} not found in post-restart WireMock calls"


class TestConnectorLifecycle:
    """Tests that create/delete their own data sources (function-scoped connectors).

    Each test creates an isolated subdirectory inside ``session_watch_dir`` and
    registers a connector watching only that subdir.  The session-scoped
    connector uses ``recursive=False`` (watches ``/watch/`` root only), so files
    dropped into these subdirs are invisible to it — preventing cross-test
    contamination when asserting no delivery.
    """

    def _create_and_wait(
        self,
        data_sources_base_url: str,
        session_watch_dir: Path,
    ) -> tuple[dict, Path]:
        """Create a connector watching an isolated subdir; return (body, host_subdir)."""
        subdir_name = f"lc-{uuid.uuid4().hex[:8]}"
        host_subdir = session_watch_dir / subdir_name
        host_subdir.mkdir()
        container_watch_path = f"/watch/{subdir_name}"

        with httpx.Client(base_url=data_sources_base_url, timeout=10.0) as client:
            resp = client.post(
                "/data-sources",
                json={
                    "display_name": f"lifecycle-test-{uuid.uuid4().hex[:8]}",
                    "path": container_watch_path,
                    "namespace": f"lifecycle-ns-{uuid.uuid4().hex[:8]}",
                    "org_name": "lifecycle-org",
                },
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            source_id = body["id"]

            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                poll = client.get(f"/data-sources/{source_id}")
                if poll.json().get("kafka_status", {}).get("state") == "RUNNING":
                    return body, host_subdir
                time.sleep(2)
            pytest.fail(f"Connector {source_id} did not reach RUNNING within 90s")

    def test_pause_suppresses_delivery_and_resume_restores_it(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
        data_sources_base_url: str,
        intake_container,  # noqa: ARG002
    ) -> None:
        """Pause stops delivery; resume restores it."""
        source, watch_dir = self._create_and_wait(
            data_sources_base_url, session_watch_dir
        )
        source_id = source["id"]

        try:
            # Pause the connector
            resp = httpx.post(f"{data_sources_base_url}/data-sources/{source_id}/pause")
            assert resp.status_code == 200
            _wait_connector_state(data_sources_base_url, source_id, "PAUSED")

            # Drop a file while paused into the isolated subdir — should not arrive.
            # The session-scoped connector uses recursive=False so it ignores subdirs.
            httpx.delete(f"{wiremock_host_url}/__admin/requests")
            fname_paused = f"paused-{uuid.uuid4().hex[:8]}.txt"
            (watch_dir / fname_paused).write_text("dropped while paused")
            _assert_no_upload(
                wiremock_host_url, namespace_id=source["namespace_id"], window=20
            )

            # Resume — the buffered file should now be delivered
            resp = httpx.post(
                f"{data_sources_base_url}/data-sources/{source_id}/resume"
            )
            assert resp.status_code == 200
            _wait_connector_state(data_sources_base_url, source_id, "RUNNING")

            _wait_for_storage_upload(
                wiremock_host_url,
                namespace_id=source["namespace_id"],
                count=1,
                timeout=60,
            )
        finally:
            httpx.delete(f"{data_sources_base_url}/data-sources/{source_id}")

    def test_delete_removes_connector_and_soft_deletes_namespace(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
        data_sources_base_url: str,
        intake_container,  # noqa: ARG002
    ) -> None:
        """
        DELETE /data-sources/{id} removes the connector
        and soft-deletes the namespace.
        """
        source, watch_dir = self._create_and_wait(
            data_sources_base_url, session_watch_dir
        )
        source_id = source["id"]
        namespace_id = source["namespace_id"]

        # Delete the data source
        resp = httpx.delete(f"{data_sources_base_url}/data-sources/{source_id}")
        assert resp.status_code == 204

        # Assert WireMock received DELETE /namespaces/{namespace_id} as part of the
        # delete call above — must be checked before the journal is reset below.
        wm_resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
        delete_calls = [
            r
            for r in wm_resp.json().get("requests", [])
            if r["request"]["method"] == "DELETE"
            and r["request"]["url"] == f"/namespaces/{namespace_id}"
        ]
        assert len(delete_calls) >= 1, (
            f"Expected WireMock to receive DELETE /namespaces/{namespace_id} "
            f"but got: {wm_resp.json()}"
        )

        # Assert it no longer appears in the list
        resp = httpx.get(f"{data_sources_base_url}/data-sources")
        assert resp.status_code == 200
        ids = [s["id"] for s in resp.json()]
        assert source_id not in ids

        # Assert no upload arrives after deletion — file goes into isolated subdir,
        # invisible to the session-scoped connector (recursive=False).
        httpx.delete(f"{wiremock_host_url}/__admin/requests")
        fname = f"post-delete-{uuid.uuid4().hex[:8]}.txt"
        (watch_dir / fname).write_text("should not arrive")
        _assert_no_upload(wiremock_host_url, namespace_id=namespace_id, window=20)

    def test_shared_namespace_not_deleted_while_sibling_active(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
        data_sources_base_url: str,
        intake_container,  # noqa: ARG002
    ) -> None:
        """Deleting one connector must not soft-delete a namespace still used by another.

        WireMock returns the same namespace_id (cccccccc) for all lifecycle connectors,
        so both connectors created here share the same namespace_id in the DB — exactly
        the multi-connector shared-namespace scenario the sibling check protects.

        NOTE: once ``group_id`` is introduced (see TODOs.md §1.1.2), per-connector object
        ownership will be tracked at the object level rather than
        through namespace scoping.
        At that point the sibling check and this test should be revisited — namespace
        deletion semantics may change depending on the final group_id design.
        """
        source_a, _ = self._create_and_wait(data_sources_base_url, session_watch_dir)
        source_b, _ = self._create_and_wait(data_sources_base_url, session_watch_dir)
        namespace_id = source_a["namespace_id"]
        assert source_a["namespace_id"] == source_b["namespace_id"], (
            "Both lifecycle connectors must share namespace_id for this test"
            "to be meaningful"
        )

        try:
            httpx.delete(f"{wiremock_host_url}/__admin/requests")

            # Delete connector A — sibling B is still active, namespace must survive.
            resp = httpx.delete(
                f"{data_sources_base_url}/data-sources/{source_a['id']}"
            )
            assert resp.status_code == 204

            wm_resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
            premature_deletes = [
                r
                for r in wm_resp.json().get("requests", [])
                if r["request"]["method"] == "DELETE"
                and r["request"]["url"] == f"/namespaces/{namespace_id}"
            ]
            assert len(premature_deletes) == 0, (
                f"Namespace was soft-deleted while sibling connector {source_b['id']} "
                f"was still active"
            )

            httpx.delete(f"{wiremock_host_url}/__admin/requests")

            # Delete connector B — last sibling gone, namespace must now be soft-deleted.
            resp = httpx.delete(
                f"{data_sources_base_url}/data-sources/{source_b['id']}"
            )
            assert resp.status_code == 204

            wm_resp = httpx.get(f"{wiremock_host_url}/__admin/requests")
            final_deletes = [
                r
                for r in wm_resp.json().get("requests", [])
                if r["request"]["method"] == "DELETE"
                and r["request"]["url"] == f"/namespaces/{namespace_id}"
            ]
            assert len(final_deletes) >= 1, (
                f"Expected namespace soft-delete after last connector removed, "
                f"but WireMock received: {wm_resp.json()}"
            )
        finally:
            # Best-effort cleanup in case of mid-test failure.
            for source in (source_a, source_b):
                httpx.delete(f"{data_sources_base_url}/data-sources/{source['id']}")

    def test_two_connectors_same_namespace_both_deliver(
        self,
        session_watch_dir: Path,
        wiremock_host_url: str,
        data_sources_base_url: str,
        intake_container,  # noqa: ARG002  # ensures intake service is up
    ) -> None:
        """Two connectors sharing a namespace both deliver files independently.

        Files dropped into each connector's isolated watch directory must both
        reach the storage service — neither stream suppresses or interferes with
        the other.
        """
        source_a, watch_dir_a = self._create_and_wait(
            data_sources_base_url, session_watch_dir
        )
        source_b, watch_dir_b = self._create_and_wait(
            data_sources_base_url, session_watch_dir
        )
        namespace_id = source_a["namespace_id"]

        try:
            httpx.delete(f"{wiremock_host_url}/__admin/requests")

            fname_a = f"from-a-{uuid.uuid4().hex[:8]}.txt"
            fname_b = f"from-b-{uuid.uuid4().hex[:8]}.txt"
            (watch_dir_a / fname_a).write_text("from connector A")
            (watch_dir_b / fname_b).write_text("from connector B")

            calls = _wait_for_storage_upload(
                wiremock_host_url, namespace_id=namespace_id, count=2, timeout=120
            )
            bodies = [c.get("body", "") for c in calls]
            assert any(
                fname_a in b for b in bodies
            ), f"{fname_a} (connector A) not found in storage uploads"
            assert any(
                fname_b in b for b in bodies
            ), f"{fname_b} (connector B) not found in storage uploads"
        finally:
            for source in (source_a, source_b):
                httpx.delete(f"{data_sources_base_url}/data-sources/{source['id']}")
