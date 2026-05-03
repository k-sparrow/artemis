"""Enterprise → ingestion full e2e test.

Proves the complete pipeline end-to-end:
  file drop → FileSource → Kafka → ksqlDB → HTTP sink → intake
            → storage → Celery → Docling → TEI → Qdrant vectors
"""

from __future__ import annotations

import uuid
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa
from fastapi import status
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue

from tests.backend.enterprise.e2e.conftest import _wait_connector_running  # noqa: PLC2701
from tests.lib.polling import poll_until

_QDRANT_COLLECTION = "artemis"

# CPU Docling is slow; 5 minutes is intentionally generous.
_QDRANT_POLL_TIMEOUT_S = 300
_QDRANT_POLL_INTERVAL_S = 5

# Qdrant vectors confirm Celery task completed; CDC just needs Debezium → JDBC sink.
_CDC_POLL_TIMEOUT_S = 60
_CDC_POLL_INTERVAL_S = 3


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

        def _scroll():
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
            return result or None

        try:
            return poll_until(
                _scroll,
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
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

    def test_ingested_objects_populated(
        self,
        qdrant_points: list,  # guarantees tasks.index has completed
        data_source: dict,
        postgres_engine: sa.Engine,
    ) -> None:
        """CDC pipeline writes ingested_objects rows for every obj_id that reached Qdrant.

        Cross-references by obj_id (not just namespace_id) so we know the exact
        objects that were indexed are the ones recorded in the CDC table.
        """
        obj_ids = [
            uuid.UUID(pt.payload["metadata"]["obj_id"])
            for pt in qdrant_points
            if pt.payload.get("metadata", {}).get("obj_id")
        ]
        assert obj_ids, "qdrant_points must carry obj_id metadata"
        # Deduplicate: one ingested_objects row per object,
        # multiple Qdrant chunks per object.
        obj_ids = list({o for o in obj_ids})

        def _rows():
            with postgres_engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                    {"ids": obj_ids},
                ).fetchall()
            return rows if len(rows) == len(obj_ids) else None

        try:
            poll_until(
                _rows, timeout=_CDC_POLL_TIMEOUT_S, interval=_CDC_POLL_INTERVAL_S
            )
        except TimeoutError:
            pytest.fail(
                f"ingested_objects missing rows for obj_ids {obj_ids} "
                f"after {_CDC_POLL_TIMEOUT_S}s"
            )

    def test_ingestion_tasks_recorded(
        self,
        qdrant_points: list,  # guarantees tasks.index has completed
        data_source: dict,
        postgres_engine: sa.Engine,
    ) -> None:
        """CDC pipeline writes ingestion_tasks rows after tasks.index SUCCESS."""
        namespace_id = uuid.UUID(data_source["namespace_id"])

        def _row():
            with postgres_engine.connect() as conn:
                return conn.execute(
                    sa.text(
                        "SELECT task_id FROM ingestion_tasks"
                        " WHERE namespace_id = :ns_id AND status = 'SUCCESS' LIMIT 1"
                    ),
                    {"ns_id": namespace_id},
                ).fetchone()

        try:
            poll_until(_row, timeout=_CDC_POLL_TIMEOUT_S, interval=_CDC_POLL_INTERVAL_S)
        except TimeoutError:
            pytest.fail(
                f"ingestion_tasks has no SUCCESS rows for namespace_id={namespace_id} "
                f"after {_CDC_POLL_TIMEOUT_S}s"
            )

    def test_storage_api_reflects_ingested_objects(
        self,
        qdrant_points: list,  # guarantees pipeline completed and CDC ran
        data_source: dict,
        storage_url: str,
    ) -> None:
        """
        Storage service GET /namespaces/{id}/objects returns the same obj_ids as Qdrant.

        The storage service reads ingested_objects directly, so this confirms the
        CDC-populated table is visible through the service's own API and that no
        ID mismatch occurred across the pipeline.
        """
        namespace_id = data_source["namespace_id"]
        obj_ids = {
            pt.payload["metadata"]["obj_id"]
            for pt in qdrant_points
            if pt.payload.get("metadata", {}).get("obj_id")
        }
        assert obj_ids, "qdrant_points must carry obj_id metadata"

        def _objects():
            resp = httpx.get(
                f"{storage_url}/namespaces/{namespace_id}/objects", timeout=10.0
            )
            resp.raise_for_status()
            found = {o["id"] for o in resp.json()}
            return found if obj_ids <= found else None

        try:
            poll_until(
                _objects, timeout=_CDC_POLL_TIMEOUT_S, interval=_CDC_POLL_INTERVAL_S
            )
        except TimeoutError:
            resp = httpx.get(
                f"{storage_url}/namespaces/{namespace_id}/objects", timeout=10.0
            )
            pytest.fail(
                f"Storage API does not reflect expected obj_ids.\n"
                f"Expected (subset): {obj_ids}\n"
                f"Got: {[o['id'] for o in resp.json()]}"
            )
