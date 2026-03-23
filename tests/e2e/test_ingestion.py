"""Tier 4 end-to-end ingestion tests.

These tests exercise the complete pipeline — parsing, embedding, vectorstore
write, and deletion — using real service containers.  No stubs are involved.

Marks: ``@pytest.mark.e2e``  (skipped unless ``--e2e`` is passed to pytest)

Prerequisites — load Docker images before running:

  bazel run //src/backend/controller/worker:image.tarball
  bazel run //src/backend/indexing:image.tarball
  bazel run //src/backend/parsing:image.tarball

Run:

  bazel test //tests/e2e/... --test_arg=--e2e
"""

from __future__ import annotations

import uuid

import pytest
from minio import Minio
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue
from testcontainers.core.container import DockerContainer

from tests.e2e.conftest import (
    E2E_COLLECTION,
    dispatch_delete_namespace,
    dispatch_ingest,
    upload_file,
    wait_for_empty,
    wait_for_points,
)


@pytest.mark.e2e
class TestIngestE2E:
    """Happy-path: document uploaded → parsed → embedded → stored in Qdrant."""

    async def test_ingest_document_appears_in_qdrant(
        self,
        dispatch_app,
        minio_client: Minio,
        qdrant_client: AsyncQdrantClient,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """After a CREATE ingest event, at least one vector must appear in Qdrant
        tagged with the correct namespace."""
        upload_file(minio_client, s3_source_bucket, "doc.md")
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc.md", namespace_id)

        await wait_for_points(
            qdrant_client, E2E_COLLECTION, namespace_id, min_count=1, timeout=120
        )

    async def test_ingest_two_files_both_appear_in_qdrant(
        self,
        dispatch_app,
        minio_client: Minio,
        qdrant_client: AsyncQdrantClient,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """Two separate ingest events in the same namespace both land in Qdrant."""
        upload_file(minio_client, s3_source_bucket, "doc_a.md")
        upload_file(minio_client, s3_source_bucket, "doc_b.md")
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc_a.md", namespace_id)
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc_b.md", namespace_id)

        # Both files produce at least one chunk each → at least 2 points total.
        await wait_for_points(
            qdrant_client, E2E_COLLECTION, namespace_id, min_count=2, timeout=120
        )


@pytest.mark.e2e
class TestDeleteDocumentE2E:
    """Single-file deletion: only the target document's vectors are removed."""

    async def test_delete_document_removes_vectors_from_qdrant(
        self,
        dispatch_app,
        minio_client: Minio,
        qdrant_client: AsyncQdrantClient,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """Ingest a file then delete it — Qdrant must end up empty for that namespace."""
        upload_file(minio_client, s3_source_bucket, "doc.md")
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc.md", namespace_id)
        await wait_for_points(
            qdrant_client, E2E_COLLECTION, namespace_id, min_count=1, timeout=120
        )

        dispatch_ingest(
            dispatch_app, s3_source_bucket, "doc.md", namespace_id, action="DELETE"
        )
        await wait_for_empty(qdrant_client, E2E_COLLECTION, namespace_id, timeout=60)


@pytest.mark.e2e
class TestDeleteNamespaceE2E:
    """Namespace-level deletion: every document in the namespace is wiped."""

    async def test_delete_namespace_wipes_all_documents(
        self,
        dispatch_app,
        minio_client: Minio,
        qdrant_client: AsyncQdrantClient,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """Ingest two files, then wipe the namespace — both must disappear."""
        upload_file(minio_client, s3_source_bucket, "doc_a.md")
        upload_file(minio_client, s3_source_bucket, "doc_b.md")
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc_a.md", namespace_id)
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc_b.md", namespace_id)
        await wait_for_points(
            qdrant_client, E2E_COLLECTION, namespace_id, min_count=2, timeout=120
        )

        dispatch_delete_namespace(dispatch_app, namespace_id)
        await wait_for_empty(qdrant_client, E2E_COLLECTION, namespace_id, timeout=60)

    async def test_delete_namespace_does_not_affect_other_namespaces(
        self,
        dispatch_app,
        minio_client: Minio,
        qdrant_client: AsyncQdrantClient,
        s3_source_bucket: str,
        worker_container: DockerContainer,
    ) -> None:
        """Deleting namespace A must not remove vectors belonging to namespace B."""
        ns_a = uuid.uuid4()
        ns_b = uuid.uuid4()

        upload_file(minio_client, s3_source_bucket, "doc_a.md")
        upload_file(minio_client, s3_source_bucket, "doc_b.md")
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc_a.md", ns_a)
        dispatch_ingest(dispatch_app, s3_source_bucket, "doc_b.md", ns_b)

        await wait_for_points(
            qdrant_client, E2E_COLLECTION, ns_a, min_count=1, timeout=120
        )
        await wait_for_points(
            qdrant_client, E2E_COLLECTION, ns_b, min_count=1, timeout=120
        )

        dispatch_delete_namespace(dispatch_app, ns_a)
        await wait_for_empty(qdrant_client, E2E_COLLECTION, ns_a, timeout=60)

        # Namespace B must still have its vectors.
        ns_b_filter = Filter(
            must=[
                FieldCondition(
                    key="metadata.namespace",
                    match=MatchValue(value=str(ns_b)),
                )
            ]
        )
        result = await qdrant_client.count(
            collection_name=E2E_COLLECTION,
            count_filter=ns_b_filter,
            exact=True,
        )
        assert (
            result.count >= 1
        ), f"Namespace B vectors were unexpectedly removed (count={result.count})"
