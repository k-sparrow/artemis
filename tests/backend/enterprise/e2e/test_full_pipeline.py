"""Enterprise → ingestion full e2e test.

Proves the complete pipeline end-to-end:
  file drop → FileSource → Kafka → ksqlDB → HTTP sink → intake
            → storage → Celery → Docling → TEI → Qdrant vectors
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi import status
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

from tests.backend.enterprise.e2e.conftest import _wait_connector_running  # noqa: PLC2701

# Maximum time to wait for at least one Qdrant vector to appear.
# CPU Docling is slow; 5 minutes is intentionally generous.
_QDRANT_POLL_TIMEOUT_S = 300
_QDRANT_POLL_INTERVAL_S = 5
_QDRANT_COLLECTION = "artemis"


@pytest.fixture(scope="session")
def data_source(
    data_sources_url: str,
    watch_dir: Path,
):
    """Create a data source connector pointing at watch_dir; delete it at teardown."""
    with httpx.Client(base_url=data_sources_url, timeout=15.0) as client:
        resp = client.post(
            "/data-sources",
            json={
                "display_name": "e2e-test-source",
                "path": "/watch",  # container path — host dir is mounted here
                "namespace": f"e2e-ns-{uuid.uuid4().hex[:8]}",
                "org_name": "e2e-org",
                "recursive": False,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        source = resp.json()

        _wait_connector_running(data_sources_url, source["id"])

        yield source

        client.delete(f"/data-sources/{source['id']}")


class TestSingleNamespaceFullPipeline:
    """Full pipeline: file drop → Qdrant vectors."""

    @pytest.fixture(scope="class")
    def qdrant_points(
        self,
        data_source: dict,
        watch_dir: Path,
        qdrant_client: QdrantClient,
    ) -> list:
        """Drop a file, poll until vectors appear, return the points."""
        namespace_id: str = data_source["namespace_id"]

        # TODO: switch to .txt once Docling from_formats is extended to include "txt".
        #       Docling Serve currently rejects plain text (400) — "md" is in the
        #       default from_formats list and is a safe stand-in for now.
        (watch_dir / "doc.md").write_text(
            "# Artemis E2E Test Document\n\n"
            "The quick brown fox jumps over the lazy dog. " * 10
        )

        deadline = time.monotonic() + _QDRANT_POLL_TIMEOUT_S
        while time.monotonic() < deadline:
            result, _ = qdrant_client.scroll(
                collection_name=_QDRANT_COLLECTION,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="metadata.namespace_id",
                            match=MatchValue(value=namespace_id),
                        )
                    ]
                ),
                limit=100,
                with_payload=True,
            )
            if result:
                return result
            time.sleep(_QDRANT_POLL_INTERVAL_S)

        pytest.fail(
            f"No vectors found in Qdrant collection '{_QDRANT_COLLECTION}' "
            f"for namespace_id={namespace_id} after {_QDRANT_POLL_TIMEOUT_S}s"
        )

    def test_vectors_exist(self, qdrant_points: list) -> None:
        assert len(qdrant_points) >= 1

    def test_namespace_matches_data_source(
        self, qdrant_points: list, data_source: dict
    ) -> None:
        namespace_id: str = data_source["namespace_id"]
        for pt in qdrant_points:
            assert pt.payload.get("metadata", {}).get("namespace_id") == namespace_id

    def test_object_not_leaked_to_other_namespaces(
        self, qdrant_points: list, qdrant_client: QdrantClient, data_source: dict
    ) -> None:
        """Each obj_id must appear under exactly one namespace.

        Queries without a namespace filter so cross-namespace leakage is visible
        even when the per-namespace query in qdrant_points looks clean.
        """
        namespace_id: str = data_source["namespace_id"]
        obj_ids = {pt.payload.get("metadata", {}).get("obj_id") for pt in qdrant_points}

        for obj_id in obj_ids:
            result, _ = qdrant_client.scroll(
                collection_name=_QDRANT_COLLECTION,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="metadata.obj_id", match=MatchValue(value=obj_id)
                        )
                    ]
                ),
                limit=200,
                with_payload=True,
            )
            namespaces = {
                pt.payload.get("metadata", {}).get("namespace_id") for pt in result
            }
            assert namespaces == {
                namespace_id
            }, f"obj_id={obj_id} found in unexpected namespaces: {namespaces}"
