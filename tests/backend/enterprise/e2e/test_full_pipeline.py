"""Enterprise → ingestion full e2e test.

Proves the complete pipeline end-to-end:
  Enterprise path:
    file drop → FileSource → Kafka → ksqlDB → HTTP sink → intake
              → storage → Celery → Docling → TEI → Qdrant vectors
              → /retrieve/invoke returns the indexed documents

  Private path:
    POST /namespaces/{id}/objects → storage → Celery → Docling → TEI → Qdrant vectors
              → /retrieve/invoke returns the indexed documents
              → CDC → ingested_objects + ingestion_tasks tables
    DELETE /namespaces/{id}/objects/{obj_id} → tombstone → Qdrant vectors gone
              → CDC → ingested_objects row deleted
"""

from __future__ import annotations

import uuid
from pathlib import Path

import lorem

import httpx
import pytest
import sqlalchemy as sa
from fastapi import status
from langchain_core.documents import Document
from langserve import RemoteRunnable
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue, SparseVector

from tests.backend.enterprise.e2e.conftest import _wait_connector_running  # noqa: PLC2701
from tests.lib.polling import poll_until

_QDRANT_COLLECTION = "artemis"

# CPU Docling is slow; 5 minutes is intentionally generous.
_QDRANT_POLL_TIMEOUT_S = 300
_QDRANT_POLL_INTERVAL_S = 5

# Qdrant vectors confirm Celery task completed; CDC just needs Debezium → JDBC sink.
_CDC_POLL_TIMEOUT_S = 60
_CDC_POLL_INTERVAL_S = 3

# Number of documents requested per retrieve call — large enough to return all chunks
# from any single document indexed during the e2e suite.
_RETRIEVE_K = 100


def _invoke_retriever(
    indexing_url: str,
    namespace_id: str,
    *,
    query: str = "document",
    group_id: str | None = None,
    k: int = _RETRIEVE_K,
) -> list[Document]:
    """Invoke the /retrieve LangServe endpoint via RemoteRunnable.

    Returns an empty list on any transient failure so the polling loop retries.
    """
    configurable: dict = {"namespace_id": namespace_id, "k": k}
    if group_id is not None:
        configurable["group_id"] = group_id
    try:
        remote: RemoteRunnable = RemoteRunnable(f"{indexing_url}/retrieve")
        return remote.invoke(query, config={"configurable": configurable})
    except Exception:
        return []


@pytest.fixture(scope="session")
def data_source(
    data_sources_url: str,
    watch_dir: Path,
):
    """
    Create a data source connector pointing at /watch/enterprise; delete at teardown.
    """
    (watch_dir / "enterprise").mkdir(exist_ok=True)
    with httpx.Client(base_url=data_sources_url, timeout=15.0) as client:
        resp = client.post(
            "/data-sources",
            json={
                "display_name": "e2e-test-source",
                "path": "/watch/enterprise",
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
    """Full pipeline: file drop → /retrieve/invoke returns indexed documents."""

    @pytest.fixture(scope="class")
    def indexed_documents(
        self,
        data_source: dict,
        watch_dir: Path,
        indexing_url: str,
    ) -> list[Document]:
        """Drop a file, poll until /retrieve/invoke returns documents, return them."""
        namespace_id: str = data_source["namespace_id"]

        # TODO: switch to .txt once Docling from_formats is extended to include "txt".
        #       Docling Serve currently rejects plain text (400) — "md" is in the
        #       default from_formats list and is a safe stand-in for now.
        (watch_dir / "enterprise" / "doc.md").write_text(
            "# Artemis E2E Test Document\n\n" + lorem.paragraph()
        )

        def _retrieve():
            docs = _invoke_retriever(indexing_url, namespace_id)
            return docs or None

        try:
            return poll_until(
                _retrieve,
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"No documents returned from /retrieve/invoke "
                f"for namespace_id={namespace_id} after {_QDRANT_POLL_TIMEOUT_S}s"
            )

    def test_documents_indexed(self, indexed_documents: list[Document]) -> None:
        assert len(indexed_documents) >= 1

    def test_namespace_matches_data_source(
        self, indexed_documents: list[Document], data_source: dict
    ) -> None:
        namespace_id: str = data_source["namespace_id"]
        for doc in indexed_documents:
            assert doc.metadata.get("namespace_id") == namespace_id

    def test_group_id_in_chunks(
        self, indexed_documents: list[Document], data_source: dict
    ) -> None:
        """Every chunk must carry the connector's group_id.

        Verifies that group_id flows through Topology A end-to-end:
        MinIO metadata → ksqlDB info struct → Celery kwargs → IngestionResult → Qdrant.
        A missing or null group_id here means delete_group will find nothing to delete.
        """
        source_id: str = data_source["id"]
        for doc in indexed_documents:
            assert doc.metadata.get("group_id") == source_id, (
                f"chunk missing or wrong group_id="
                f"{doc.metadata.get('group_id')!r}, expected {source_id!r}"
            )

    def test_object_not_leaked_to_other_namespaces(
        self,
        indexed_documents: list[Document],
        qdrant_client: QdrantClient,
        data_source: dict,
    ) -> None:
        """Each obj_id must appear under exactly one namespace.

        Queries Qdrant directly (without a namespace filter) so cross-namespace
        leakage is visible even when the per-namespace retriever query looks clean.
        The retriever always scopes by namespace_id so it cannot catch this class
        of leak — Qdrant scroll is the right tool here.
        """
        namespace_id: str = data_source["namespace_id"]
        obj_ids = {doc.metadata.get("obj_id") for doc in indexed_documents}

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
        indexed_documents: list[Document],
        data_source: dict,
        postgres_engine: sa.Engine,
    ) -> None:
        """CDC pipeline writes ingested_objects rows for every obj_id that reached Qdrant.

        Cross-references by obj_id (not just namespace_id) so we know the exact
        objects that were indexed are the ones recorded in the CDC table.
        """
        obj_ids = list(
            {
                uuid.UUID(doc.metadata["obj_id"])
                for doc in indexed_documents
                if doc.metadata.get("obj_id")
            }
        )
        assert obj_ids, "indexed_documents must carry obj_id metadata"

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
        indexed_documents: list[Document],
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
        indexed_documents: list[Document],
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
            doc.metadata["obj_id"]
            for doc in indexed_documents
            if doc.metadata.get("obj_id")
        }
        assert obj_ids, "indexed_documents must carry obj_id metadata"

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

    def test_hybrid_sparse_vectors_stored(
        self,
        indexed_documents: list[Document],
        data_source: dict,
        qdrant_client: QdrantClient,
        retrieval_mode: str,
    ) -> None:
        """In hybrid mode every stored point must carry a non-empty BM25 sparse vector.

        Skipped when running in dense mode — dense collections have no sparse
        vector field and the assertion would always fail there.

        Uses Qdrant directly because the retriever does not expose raw vector
        payloads — this is an infrastructure-level assertion about storage format,
        not about retrieval correctness.
        """
        if retrieval_mode != "hybrid":
            pytest.skip("sparse vector check is only meaningful in hybrid mode")

        namespace_id: str = data_source["namespace_id"]
        points, _ = qdrant_client.scroll(
            collection_name=_QDRANT_COLLECTION,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="metadata.namespace_id",
                        match=MatchValue(value=namespace_id),
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
            sparse: SparseVector = vectors["sparse"]
            assert (
                len(sparse.indices) > 0
            ), f"Point {point.id} has an empty sparse vector (no BM25 terms indexed)"


class TestPrivatePathFullPipeline:
    """Private upload path: POST /objects → retrieve → CDC → DELETE → cleanup."""

    @pytest.fixture(scope="class")
    def namespace(self, storage_url: str) -> dict:
        """Create a PRIVATE namespace; return the response dict."""
        resp = httpx.post(
            f"{storage_url}/namespaces",
            json={"type": "private"},
            headers={"X-Owner-Id": str(uuid.uuid4())},
            timeout=10.0,
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        return resp.json()

    @pytest.fixture(scope="class")
    def indexed_documents(
        self,
        namespace: dict,
        storage_url: str,
        indexing_url: str,
    ) -> list[Document]:
        """Upload a file directly to the storage service and poll until retrievable."""
        namespace_id: str = str(namespace["id"])

        content = "# Artemis Private Path E2E Test\n\n" + lorem.paragraph()
        resp = httpx.post(
            f"{storage_url}/namespaces/{namespace_id}/objects",
            files={"file": ("private_doc.md", content.encode(), "text/markdown")},
            timeout=15.0,
        )
        assert resp.status_code == status.HTTP_202_ACCEPTED, resp.text

        def _retrieve():
            docs = _invoke_retriever(indexing_url, namespace_id)
            return docs or None

        try:
            return poll_until(
                _retrieve,
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"No documents returned from /retrieve/invoke "
                f"for private namespace_id={namespace_id} after {_QDRANT_POLL_TIMEOUT_S}s"
            )

    def test_documents_indexed(self, indexed_documents: list[Document]) -> None:
        assert len(indexed_documents) >= 1

    def test_namespace_matches(
        self, indexed_documents: list[Document], namespace: dict
    ) -> None:
        namespace_id = str(namespace["id"])
        for doc in indexed_documents:
            assert doc.metadata.get("namespace_id") == namespace_id

    def test_group_id_absent_for_private_uploads(
        self, indexed_documents: list[Document]
    ) -> None:
        """Private uploads carry no group_id — only enterprise connectors set one.

        Verifies that Topology A's group_id injection (MinIO metadata → ksqlDB info
        struct) does not bleed into the private upload path where no connector is
        involved and no group_id is provided in the ingestion request.
        """
        for doc in indexed_documents:
            group_id = doc.metadata.get("group_id")
            assert (
                group_id is None
            ), f"chunk unexpectedly has group_id={group_id!r} on a private upload path"

    def test_ingested_objects_populated(
        self,
        indexed_documents: list[Document],
        postgres_engine: sa.Engine,
    ) -> None:
        obj_ids = list(
            {
                uuid.UUID(doc.metadata["obj_id"])
                for doc in indexed_documents
                if doc.metadata.get("obj_id")
            }
        )
        assert obj_ids

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
        indexed_documents: list[Document],
        namespace: dict,
        postgres_engine: sa.Engine,
    ) -> None:
        namespace_id = uuid.UUID(namespace["id"])

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

    def test_delete_removes_vectors_and_db_row(
        self,
        indexed_documents: list[Document],
        namespace: dict,
        storage_url: str,
        qdrant_client: QdrantClient,
        postgres_engine: sa.Engine,
    ) -> None:
        """
        DELETE /objects/{obj_id} → tombstone → Qdrant empty + ingested_objects row gone.
        """
        namespace_id = str(namespace["id"])
        obj_ids = list(
            {
                doc.metadata["obj_id"]
                for doc in indexed_documents
                if doc.metadata.get("obj_id")
            }
        )
        assert len(obj_ids) == 1, f"expected exactly one object, got {obj_ids}"
        obj_id = obj_ids[0]

        resp = httpx.delete(
            f"{storage_url}/namespaces/{namespace_id}/objects/{obj_id}",
            timeout=10.0,
        )
        assert resp.status_code == status.HTTP_202_ACCEPTED, resp.text

        # Qdrant: all chunks for this object must disappear.
        # Uses Qdrant directly because delete is an infra-level operation — the
        # retriever would also return 0 results after deletion, but a scroll
        # gives a definitive "nothing stored" signal regardless of query matching.
        def _qdrant_empty():
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
                limit=1,
                with_payload=False,
            )
            return True if not result else None

        try:
            poll_until(
                _qdrant_empty,
                timeout=_CDC_POLL_TIMEOUT_S,
                interval=_CDC_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"Qdrant still has vectors for namespace_id={namespace_id} "
                f"after DELETE and {_CDC_POLL_TIMEOUT_S}s"
            )

        # CDC: ingested_objects row must be removed by the tombstone sink
        def _db_row_gone():
            with postgres_engine.connect() as conn:
                row = conn.execute(
                    sa.text("SELECT id FROM ingested_objects WHERE id = :id"),
                    {"id": uuid.UUID(obj_id)},
                ).fetchone()
            return True if row is None else None

        try:
            poll_until(
                _db_row_gone,
                timeout=_CDC_POLL_TIMEOUT_S,
                interval=_CDC_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"ingested_objects row for obj_id={obj_id} still present "
                f"after DELETE and {_CDC_POLL_TIMEOUT_S}s"
            )

        # Storage API: object must no longer appear in the namespace listing
        resp = httpx.get(
            f"{storage_url}/namespaces/{namespace_id}/objects", timeout=10.0
        )
        resp.raise_for_status()
        found_ids = {o["id"] for o in resp.json()}
        assert (
            obj_id not in found_ids
        ), f"Storage API still lists obj_id={obj_id} after DELETE"


class TestConnectorDeleteCleansUpGroup:
    """Deleting a data source removes all its objects from Qdrant and ingested_objects.

    Uses a dedicated data source (not the session-scoped one) so teardown
    doesn't interfere with TestSingleNamespaceFullPipeline.
    """

    @pytest.fixture(scope="class")
    def connector_data_source(
        self,
        data_sources_url: str,
        watch_dir: Path,
    ) -> dict:
        """Create a dedicated data source for this class; do NOT delete at teardown
        — the test itself deletes it as the action under test."""
        (watch_dir / "connector-delete").mkdir(exist_ok=True)
        with httpx.Client(base_url=data_sources_url, timeout=15.0) as client:
            resp = client.post(
                "/data-sources",
                json={
                    "display_name": "e2e-connector-delete-source",
                    "path": "/watch/connector-delete",
                    "namespace": f"e2e-connector-delete-ns-{uuid.uuid4().hex[:8]}",
                    "org_name": "e2e-org",
                    "recursive": False,
                },
            )
            assert resp.status_code == status.HTTP_201_CREATED, resp.text
            source = resp.json()
            _wait_connector_running(data_sources_url, source["id"])
            return source

    @pytest.fixture(scope="class")
    def indexed_documents(
        self,
        connector_data_source: dict,
        watch_dir: Path,
        indexing_url: str,
    ) -> list[Document]:
        """Drop TWO files; poll via /retrieve/invoke until 2 distinct obj_ids appear."""
        namespace_id: str = connector_data_source["namespace_id"]

        base = watch_dir / "connector-delete"
        (base / "doc_alpha.md").write_text(
            "# Connector Delete E2E — Doc Alpha\n\n" + lorem.paragraph()
        )
        (base / "doc_beta.md").write_text(
            "# Connector Delete E2E — Doc Beta\n\n" + lorem.paragraph()
        )

        def _retrieve():
            docs = _invoke_retriever(indexing_url, namespace_id)
            obj_ids = {doc.metadata.get("obj_id") for doc in docs}
            return docs if len(obj_ids) == 2 else None

        try:
            return poll_until(
                _retrieve,
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"Did not get 2 distinct obj_ids from /retrieve/invoke "
                f"for connector-delete namespace_id={namespace_id} "
                f"after {_QDRANT_POLL_TIMEOUT_S}s"
            )

    def test_group_id_in_chunks(
        self, indexed_documents: list[Document], connector_data_source: dict
    ) -> None:
        """
        Every chunk produced by this connector must carry its connector id as group_id.

        This is the same invariant as
        TestSingleNamespaceFullPipeline.test_group_id_in_chunks but verified on the
        dedicated connector used by TestConnectorDeleteCleansUpGroup,
        ensuring the group_id is set before we attempt to delete by group.
        """
        source_id: str = connector_data_source["id"]
        for doc in indexed_documents:
            assert doc.metadata.get("group_id") == source_id, (
                f"chunk missing or wrong group_id="
                f"{doc.metadata.get('group_id')!r}, expected {source_id!r}"
            )

    def test_delete_connector_removes_all_group_objects(
        self,
        indexed_documents: list[Document],
        connector_data_source: dict,
        data_sources_url: str,
        storage_url: str,
        qdrant_client: QdrantClient,
        postgres_engine: sa.Engine,
    ) -> None:
        """DELETE /data-sources/{id} → group delete → Qdrant empty + DB rows gone."""
        source_id = connector_data_source["id"]
        namespace_id = connector_data_source["namespace_id"]
        obj_ids = {
            doc.metadata["obj_id"]
            for doc in indexed_documents
            if doc.metadata.get("obj_id")
        }
        assert obj_ids
        obj_uuids = [uuid.UUID(oid) for oid in obj_ids]

        # Gate on CDC having populated ingested_objects before issuing the delete.
        # delete_group queries that table; if CDC hasn't landed yet it finds nothing
        # and dispatches no tombstone tasks, leaving Qdrant untouched.
        def _cdc_ready():
            with postgres_engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                    {"ids": obj_uuids},
                ).fetchall()
            return rows if len(rows) == len(obj_uuids) else None

        try:
            poll_until(
                _cdc_ready, timeout=_CDC_POLL_TIMEOUT_S, interval=_CDC_POLL_INTERVAL_S
            )
        except TimeoutError:
            pytest.fail(
                f"ingested_objects not populated within {_CDC_POLL_TIMEOUT_S}s "
                f"— cannot safely issue connector DELETE"
            )

        resp = httpx.delete(
            f"{data_sources_url}/data-sources/{source_id}", timeout=15.0
        )
        assert resp.status_code == status.HTTP_204_NO_CONTENT, resp.text

        # Qdrant: all chunks for this namespace must disappear
        def _qdrant_empty():
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
                limit=1,
                with_payload=False,
            )
            return True if not result else None

        try:
            poll_until(
                _qdrant_empty,
                timeout=_CDC_POLL_TIMEOUT_S,
                interval=_CDC_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"Qdrant still has vectors for namespace_id={namespace_id} "
                f"after connector DELETE and {_CDC_POLL_TIMEOUT_S}s"
            )

        # CDC: ingested_objects rows for all obj_ids must be gone
        def _db_rows_gone():
            with postgres_engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                    {"ids": obj_uuids},
                ).fetchall()
            return True if not rows else None

        try:
            poll_until(
                _db_rows_gone,
                timeout=_CDC_POLL_TIMEOUT_S,
                interval=_CDC_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"ingested_objects rows still present for obj_ids={obj_uuids} "
                f"after connector DELETE and {_CDC_POLL_TIMEOUT_S}s"
            )

        # Storage API: namespace must be empty or gone.
        # delete_data_source soft-deletes the namespace when no siblings share it,
        # so a 404 is the expected outcome here and counts as a clean state.
        resp = httpx.get(
            f"{storage_url}/namespaces/{namespace_id}/objects", timeout=10.0
        )
        if resp.status_code == status.HTTP_404_NOT_FOUND:
            return  # namespace itself was deleted — strongest possible confirmation
        resp.raise_for_status()
        assert resp.json() == [], (
            f"Storage API still lists objects for namespace_id={namespace_id} "
            f"after connector DELETE: {resp.json()}"
        )


class TestSharedNamespaceTwoConnectors:
    """Two connectors share one namespace; each owns a distinct group_id.

    Deleting connector A must remove only A's group; B's vectors and
    ingested_objects rows must survive and the namespace must stay alive.
    Deleting connector B afterwards must finish the cleanup and soft-delete
    the namespace.
    """

    @pytest.fixture(scope="class")
    def shared_namespace_name(self) -> str:
        return f"e2e-shared-ns-{uuid.uuid4().hex[:8]}"

    @pytest.fixture(scope="class")
    def connector_a(
        self,
        data_sources_url: str,
        watch_dir: Path,
        shared_namespace_name: str,
    ) -> dict:
        (watch_dir / "shared-a").mkdir(exist_ok=True)
        with httpx.Client(base_url=data_sources_url, timeout=15.0) as client:
            resp = client.post(
                "/data-sources",
                json={
                    "display_name": "e2e-shared-connector-a",
                    "path": "/watch/shared-a",
                    "namespace": shared_namespace_name,
                    "org_name": "e2e-org",
                    "recursive": False,
                },
            )
            assert resp.status_code == status.HTTP_201_CREATED, resp.text
            source = resp.json()
            _wait_connector_running(data_sources_url, source["id"])
            return source

    @pytest.fixture(scope="class")
    def connector_b(
        self,
        data_sources_url: str,
        watch_dir: Path,
        shared_namespace_name: str,
    ) -> dict:
        (watch_dir / "shared-b").mkdir(exist_ok=True)
        with httpx.Client(base_url=data_sources_url, timeout=15.0) as client:
            resp = client.post(
                "/data-sources",
                json={
                    "display_name": "e2e-shared-connector-b",
                    "path": "/watch/shared-b",
                    "namespace": shared_namespace_name,
                    "org_name": "e2e-org",
                    "recursive": False,
                },
            )
            assert resp.status_code == status.HTTP_201_CREATED, resp.text
            source = resp.json()
            _wait_connector_running(data_sources_url, source["id"])
            return source

    def _retrieve_by_group(
        self,
        indexing_url: str,
        namespace_id: str,
        group_id: str,
        min_obj_ids: int = 1,
    ) -> list[Document] | None:
        """Query /retrieve/invoke scoped to a specific group_id; return docs when
        at least ``min_obj_ids`` distinct obj_ids appear, else None."""
        docs = _invoke_retriever(indexing_url, namespace_id, group_id=group_id)
        obj_ids = {doc.metadata.get("obj_id") for doc in docs}
        return docs if len(obj_ids) >= min_obj_ids else None

    @pytest.fixture(scope="class")
    def indexed_documents_a(
        self,
        connector_a: dict,
        watch_dir: Path,
        indexing_url: str,
    ) -> list[Document]:
        """Drop 2 files for connector A; wait until exactly 2 obj_ids appear."""
        namespace_id = connector_a["namespace_id"]
        group_id = connector_a["id"]
        base = watch_dir / "shared-a"
        (base / "doc_a1.md").write_text(
            "# Shared Namespace — Connector A, Doc 1\n\n" + lorem.paragraph()
        )
        (base / "doc_a2.md").write_text(
            "# Shared Namespace — Connector A, Doc 2\n\n" + lorem.paragraph()
        )

        try:
            return poll_until(
                lambda: self._retrieve_by_group(
                    indexing_url, namespace_id, group_id, min_obj_ids=2
                ),
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"connector_a docs not retrievable after {_QDRANT_POLL_TIMEOUT_S}s"
            )

    @pytest.fixture(scope="class")
    def indexed_documents_b(
        self,
        connector_b: dict,
        watch_dir: Path,
        indexing_url: str,
    ) -> list[Document]:
        """Drop 2 files for connector B; wait until exactly 2 obj_ids appear."""
        namespace_id = connector_b["namespace_id"]
        group_id = connector_b["id"]
        base = watch_dir / "shared-b"
        (base / "doc_b1.md").write_text(
            "# Shared Namespace — Connector B, Doc 1\n\n" + lorem.paragraph()
        )
        (base / "doc_b2.md").write_text(
            "# Shared Namespace — Connector B, Doc 2\n\n" + lorem.paragraph()
        )

        try:
            return poll_until(
                lambda: self._retrieve_by_group(
                    indexing_url, namespace_id, group_id, min_obj_ids=2
                ),
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"connector_b docs not retrievable after {_QDRANT_POLL_TIMEOUT_S}s"
            )

    def _obj_uuids(self, documents: list[Document]) -> list[uuid.UUID]:
        return list(
            {
                uuid.UUID(doc.metadata["obj_id"])
                for doc in documents
                if doc.metadata.get("obj_id")
            }
        )

    def _wait_cdc(
        self,
        postgres_engine: sa.Engine,
        obj_uuids: list[uuid.UUID],
        label: str,
    ) -> None:
        def _rows():
            with postgres_engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                    {"ids": obj_uuids},
                ).fetchall()
            return rows if len(rows) == len(obj_uuids) else None

        try:
            poll_until(
                _rows, timeout=_CDC_POLL_TIMEOUT_S, interval=_CDC_POLL_INTERVAL_S
            )
        except TimeoutError:
            pytest.fail(
                f"ingested_objects not populated for {label} "
                f"within {_CDC_POLL_TIMEOUT_S}s"
            )

    # ------------------------------------------------------------------
    # Tests run in definition order; each builds on the previous state.
    # ------------------------------------------------------------------

    def test_both_groups_indexed(
        self,
        indexed_documents_a: list[Document],
        indexed_documents_b: list[Document],
        connector_a: dict,
        connector_b: dict,
    ) -> None:
        """Sanity: every chunk belongs to exactly one of the two connectors."""
        group_a = connector_a["id"]
        group_b = connector_b["id"]
        group_ids_a = {doc.metadata.get("group_id") for doc in indexed_documents_a}
        group_ids_b = {doc.metadata.get("group_id") for doc in indexed_documents_b}
        assert group_ids_a == {
            group_a
        }, f"unexpected group_ids in connector_a chunks: {group_ids_a}"
        assert group_ids_b == {
            group_b
        }, f"unexpected group_ids in connector_b chunks: {group_ids_b}"

    def test_delete_connector_a_cleans_only_its_group(
        self,
        indexed_documents_a: list[Document],
        indexed_documents_b: list[Document],
        connector_a: dict,
        connector_b: dict,
        data_sources_url: str,
        storage_url: str,
        qdrant_client: QdrantClient,
        indexing_url: str,
        postgres_engine: sa.Engine,
    ) -> None:
        """DELETE connector A removes A's group; B's chunks and DB rows survive."""
        group_a = connector_a["id"]
        group_b = connector_b["id"]
        namespace_id = connector_a["namespace_id"]
        obj_uuids_a = self._obj_uuids(indexed_documents_a)
        obj_uuids_b = self._obj_uuids(indexed_documents_b)

        # Gate: both groups must be in ingested_objects before the delete.
        self._wait_cdc(postgres_engine, obj_uuids_a, "connector_a")
        self._wait_cdc(postgres_engine, obj_uuids_b, "connector_b")

        resp = httpx.delete(f"{data_sources_url}/data-sources/{group_a}", timeout=15.0)
        assert resp.status_code == status.HTTP_204_NO_CONTENT, resp.text

        # Qdrant: connector A's chunks must disappear.
        def _a_gone():
            result, _ = qdrant_client.scroll(
                collection_name=_QDRANT_COLLECTION,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="metadata.group_id",
                            match=MatchValue(value=group_a),
                        )
                    ]
                ),
                limit=1,
                with_payload=False,
            )
            return True if not result else None

        try:
            poll_until(
                _a_gone, timeout=_CDC_POLL_TIMEOUT_S, interval=_CDC_POLL_INTERVAL_S
            )
        except TimeoutError:
            pytest.fail(
                f"Qdrant still has chunks for group_a={group_a} "
                f"after {_CDC_POLL_TIMEOUT_S}s"
            )

        # Retriever: connector B's chunks must still be retrievable.
        b_docs = self._retrieve_by_group(
            indexing_url, namespace_id, group_b, min_obj_ids=2
        )
        assert b_docs, "connector_b docs unexpectedly gone after deleting connector_a"

        # ingested_objects: A's rows gone, B's rows survive.
        with postgres_engine.connect() as conn:
            a_rows = conn.execute(
                sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                {"ids": obj_uuids_a},
            ).fetchall()
            b_rows = conn.execute(
                sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                {"ids": obj_uuids_b},
            ).fetchall()
        assert not a_rows, f"ingested_objects still has rows for connector_a: {a_rows}"
        assert len(b_rows) == len(obj_uuids_b), (
            f"ingested_objects lost connector_b rows after deleting connector_a: "
            f"expected {len(obj_uuids_b)}, got {len(b_rows)}"
        )

        # Namespace must still be accessible (connector B is still alive).
        resp = httpx.get(
            f"{storage_url}/namespaces/{namespace_id}/objects", timeout=10.0
        )
        assert (
            resp.status_code == status.HTTP_200_OK
        ), f"Namespace was soft-deleted prematurely: {resp.status_code}"

    def test_delete_connector_b_cleans_up_namespace(
        self,
        indexed_documents_b: list[Document],
        connector_b: dict,
        data_sources_url: str,
        storage_url: str,
        qdrant_client: QdrantClient,
        postgres_engine: sa.Engine,
    ) -> None:
        """DELETE connector B removes the last group and soft-deletes the namespace."""
        group_b = connector_b["id"]
        namespace_id = connector_b["namespace_id"]
        obj_uuids_b = self._obj_uuids(indexed_documents_b)

        resp = httpx.delete(f"{data_sources_url}/data-sources/{group_b}", timeout=15.0)
        assert resp.status_code == status.HTTP_204_NO_CONTENT, resp.text

        # Qdrant: namespace must be fully empty.
        def _namespace_empty():
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
                limit=1,
                with_payload=False,
            )
            return True if not result else None

        try:
            poll_until(
                _namespace_empty,
                timeout=_CDC_POLL_TIMEOUT_S,
                interval=_CDC_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"Qdrant still has chunks for namespace_id={namespace_id} "
                f"after deleting connector_b and {_CDC_POLL_TIMEOUT_S}s"
            )

        # ingested_objects: B's rows must be gone.
        def _b_rows_gone():
            with postgres_engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT id FROM ingested_objects WHERE id = ANY(:ids)"),
                    {"ids": obj_uuids_b},
                ).fetchall()
            return True if not rows else None

        try:
            poll_until(
                _b_rows_gone,
                timeout=_CDC_POLL_TIMEOUT_S,
                interval=_CDC_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"ingested_objects still has rows for connector_b "
                f"after {_CDC_POLL_TIMEOUT_S}s"
            )

        # Namespace must be gone (no siblings left → soft-deleted).
        resp = httpx.get(
            f"{storage_url}/namespaces/{namespace_id}/objects", timeout=10.0
        )
        assert (
            resp.status_code == status.HTTP_404_NOT_FOUND
        ), f"Expected namespace to be soft-deleted (404), got {resp.status_code}"


class TestColBERTReranking:
    """ColBERT reranker changes the ordering of retrieved documents.

    Skipped when reranker_mode == "none".  Requires a GPU-backed VLLMContainer
    (started by the reranker_mode fixture in conftest.py).

    Uses the enterprise pipeline (file drop → Qdrant) as the ingestion path,
    then queries /retrieve/invoke twice — once with a keyword query that BM25
    would naturally rank first and once with a semantic query.  With ColBERT
    reranking active the semantic query must rank the semantically relevant
    document ahead of the keyword-heavy one.

    Documents are synthetic so that the expected ranking is unambiguous:
      keyword_doc   — contains a rare out-of-vocabulary token (BM25 signal)
      semantic_doc  — describes transformer self-attention (semantic signal)
    """

    _KEYWORD = "ZQXVBYW"  # synthetic OOV token — no embedding model has seen it

    @pytest.fixture(scope="class")
    def reranker_namespace(
        self,
        reranker_mode: str,
        data_sources_url: str,
        watch_dir: Path,
        indexing_url: str,
    ) -> dict:
        """Create a data source and index two documents; return namespace metadata.

        Skips the class immediately when the reranker is not enabled so none of
        the tests in this class run in the no-reranker parametrize leg.
        """
        if reranker_mode == "none":
            pytest.skip("ColBERT reranker not enabled in this parametrize leg")

        (watch_dir / "reranker").mkdir(exist_ok=True)
        with httpx.Client(base_url=data_sources_url, timeout=15.0) as client:
            resp = client.post(
                "/data-sources",
                json={
                    "display_name": "e2e-reranker-source",
                    "path": "/watch/reranker",
                    "namespace": f"e2e-reranker-{uuid.uuid4().hex[:8]}",
                    "org_name": "e2e-org",
                    "recursive": False,
                },
            )
            assert resp.status_code == status.HTTP_201_CREATED, resp.text
            source = resp.json()

        namespace_id: str = source["namespace_id"]

        (watch_dir / "reranker" / "keyword_doc.md").write_text(
            f"# Keyword Document\n\n"
            f"{self._KEYWORD} is a unique synthetic token used to test BM25 keyword "
            f"matching. This document should rank first for "
            f"a query containing {self._KEYWORD}. " * 5
        )
        (watch_dir / "reranker" / "semantic_doc.md").write_text(
            "# Transformer Architecture\n\n"
            "The transformer architecture uses self-attention to model long-range "
            "dependencies in sequences. Multi-head attention allows the model to "
            "jointly attend to information from different representation subspaces. "
            * 5
        )

        def _both_indexed():
            docs = _invoke_retriever(indexing_url, namespace_id)
            obj_ids = {doc.metadata.get("obj_id") for doc in docs}
            return docs if len(obj_ids) >= 2 else None

        try:
            poll_until(
                _both_indexed,
                timeout=_QDRANT_POLL_TIMEOUT_S,
                interval=_QDRANT_POLL_INTERVAL_S,
            )
        except TimeoutError:
            pytest.fail(
                f"reranker test docs not indexed within {_QDRANT_POLL_TIMEOUT_S}s"
            )

        return {"namespace_id": namespace_id, "source_id": source["id"]}

    def test_semantic_query_ranks_semantic_doc_first(
        self,
        reranker_namespace: dict,
        indexing_url: str,
    ) -> None:
        """
        A semantic query must rank semantic_doc above
        keyword_doc after ColBERT reranking.
        """
        namespace_id = reranker_namespace["namespace_id"]
        docs = _invoke_retriever(
            indexing_url,
            namespace_id,
            query="how does attention mechanism work in neural networks",
            k=10,
        )
        assert docs, "no documents returned for semantic query"

        top_content = docs[0].page_content
        assert (
            "self-attention" in top_content or "transformer" in top_content.lower()
        ), (
            f"Expected semantic_doc at rank 1 after ColBERT reranking, "
            f"got: {top_content[:120]!r}"
        )

    def test_keyword_query_ranks_keyword_doc_first(
        self,
        reranker_namespace: dict,
        indexing_url: str,
    ) -> None:
        """An exact OOV-token query must rank keyword_doc first (BM25 + ColBERT agree)."""
        namespace_id = reranker_namespace["namespace_id"]
        docs = _invoke_retriever(indexing_url, namespace_id, k=10)
        assert docs, "no documents returned for keyword query"

        # Re-invoke with the keyword as the actual query string
        try:
            from langserve import RemoteRunnable

            remote: RemoteRunnable = RemoteRunnable(f"{indexing_url}/retrieve")
            keyword_docs = remote.invoke(
                self._KEYWORD,
                config={"configurable": {"namespace_id": namespace_id, "k": 10}},
            )
        except Exception as exc:
            pytest.fail(f"keyword query failed: {exc}")

        assert keyword_docs, "no documents returned for keyword query"
        top_content = keyword_docs[0].page_content
        assert self._KEYWORD in top_content, (
            f"Expected keyword_doc at rank 1 for OOV query {self._KEYWORD!r}, "
            f"got: {top_content[:120]!r}"
        )
