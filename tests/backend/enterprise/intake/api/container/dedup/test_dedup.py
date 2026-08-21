"""Container tests: the intake service's content-addressed dedup ledger.

Verifies the actual defense-in-depth mechanism designed against connector-level
duplication (eviction, restarts, tasks.max>1, at-least-once redelivery — all of
which redeliver the same (path, content) pair):
  - A duplicate (namespace, path, content) short-circuits without a second
    upload to the storage service.
  - Same path, different content is NOT deduped (a legitimate update).
  - Different path, same content is NOT deduped (two distinct documents that
    happen to share bytes must stay distinct).
  - A symlink and its target collapse to the same claim (canonical-path
    resolution catches what path-based connector idempotency misses).
  - A symlink escaping the watch root is rejected, not silently followed.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import httpx
import pytest  # noqa: F401
from fastapi import status

from tests.backend.enterprise.intake.api.container.dedup.conftest import (
    _NAMESPACE_ID,
    _OWNER_ID,
    _TASK_ID,
)

_UPLOAD_PATH_SUFFIX = "/objects"


def _upload_count(wiremock_admin_url: str) -> int:
    resp = httpx.get(f"{wiremock_admin_url}/__admin/requests", timeout=5.0)
    resp.raise_for_status()
    return sum(
        1
        for r in resp.json().get("requests", [])
        if r["request"]["method"] == "POST"
        and r["request"]["url"].endswith(_UPLOAD_PATH_SUFFIX)
    )


def _intake(base_url: str, path: str, display_name: str) -> httpx.Response:
    return httpx.post(
        f"{base_url}/intake",
        json={
            "source": {"type": "filesystem", "path": path},
            "display_name": display_name,
            "namespace_id": _NAMESPACE_ID,
            "owner_id": _OWNER_ID,
        },
        timeout=10.0,
    )


class TestDuplicateUploadShortCircuits:
    def test_second_identical_request_reuses_task_id_without_reupload(
        self, intake_base_url: str, wiremock_admin_url: str, watch_dir: Path
    ) -> None:
        fname = f"dup-{uuid.uuid4().hex[:8]}.txt"
        (watch_dir / fname).write_text("identical content")
        container_path = f"/watch/{fname}"

        before = _upload_count(wiremock_admin_url)

        resp1 = _intake(intake_base_url, container_path, fname)
        resp2 = _intake(intake_base_url, container_path, fname)

        assert resp1.status_code == status.HTTP_202_ACCEPTED
        assert resp2.status_code == status.HTTP_202_ACCEPTED
        assert resp1.json()["task_id"] == resp2.json()["task_id"] == _TASK_ID

        after = _upload_count(wiremock_admin_url)
        assert after - before == 1, "second request should not have re-uploaded"


class TestSamePathDifferentContentNotDeduped:
    def test_content_change_at_same_path_triggers_new_upload(
        self, intake_base_url: str, wiremock_admin_url: str, watch_dir: Path
    ) -> None:
        fname = f"changed-{uuid.uuid4().hex[:8]}.txt"
        f = watch_dir / fname
        container_path = f"/watch/{fname}"

        f.write_text("version one")
        before = _upload_count(wiremock_admin_url)
        resp1 = _intake(intake_base_url, container_path, fname)
        assert resp1.status_code == status.HTTP_202_ACCEPTED

        f.write_text("version two — genuinely different bytes")
        resp2 = _intake(intake_base_url, container_path, fname)
        assert resp2.status_code == status.HTTP_202_ACCEPTED

        after = _upload_count(wiremock_admin_url)
        assert after - before == 2, "different content at the same path must not dedupe"


class TestDifferentPathSameContentNotDeduped:
    def test_two_distinct_files_with_identical_bytes_both_upload(
        self, intake_base_url: str, wiremock_admin_url: str, watch_dir: Path
    ) -> None:
        subdir = watch_dir / f"distinct-{uuid.uuid4().hex[:8]}"
        subdir.mkdir()
        content = "byte-identical boilerplate"
        (subdir / "a.txt").write_text(content)
        (subdir / "b.txt").write_text(content)

        before = _upload_count(wiremock_admin_url)
        resp_a = _intake(intake_base_url, f"/watch/{subdir.name}/a.txt", "a.txt")
        resp_b = _intake(intake_base_url, f"/watch/{subdir.name}/b.txt", "b.txt")
        assert resp_a.status_code == status.HTTP_202_ACCEPTED
        assert resp_b.status_code == status.HTTP_202_ACCEPTED

        after = _upload_count(wiremock_admin_url)
        assert after - before == 2, (
            "two distinct files that happen to share content must both upload "
            "— this is the false-positive a path-less dedup key would cause"
        )


class TestSymlinkResolution:
    def test_symlink_and_target_collapse_to_same_claim(
        self, intake_base_url: str, wiremock_admin_url: str, watch_dir: Path
    ) -> None:
        subdir = watch_dir / f"symlink-{uuid.uuid4().hex[:8]}"
        subdir.mkdir()
        real = subdir / "real.txt"
        real.write_text("shared underlying bytes")
        link = subdir / "link.txt"
        # Relative target: an absolute host path (e.g. from tempfile.mkdtemp())
        # wouldn't resolve inside the container, which only sees /watch/...
        os.symlink("real.txt", link)

        before = _upload_count(wiremock_admin_url)
        resp_real = _intake(
            intake_base_url, f"/watch/{subdir.name}/real.txt", "real.txt"
        )
        resp_link = _intake(
            intake_base_url, f"/watch/{subdir.name}/link.txt", "link.txt"
        )
        assert resp_real.status_code == status.HTTP_202_ACCEPTED
        assert resp_link.status_code == status.HTTP_202_ACCEPTED
        assert resp_real.json()["task_id"] == resp_link.json()["task_id"]

        after = _upload_count(wiremock_admin_url)
        assert after - before == 1, (
            "a symlink and its target are the same underlying document and "
            "must collapse to one claim, even though the connector layer "
            "would treat them as two different files"
        )

    def test_symlink_escaping_watch_root_is_rejected(
        self, intake_base_url: str, watch_dir: Path
    ) -> None:
        link = watch_dir / f"escape-{uuid.uuid4().hex[:8]}.txt"
        # Absolute target outside /watch, but a real, readable file inside
        # the intake container's own filesystem — the realistic risk, not a
        # symlink to a path that simply doesn't exist.
        os.symlink("/etc/hostname", link)

        resp = _intake(intake_base_url, f"/watch/{link.name}", link.name)

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "watch root" in resp.json()["detail"].lower()
