"""Tier 3 chain integration tests for the Celery worker.

These tests start a real Celery worker subprocess alongside real RabbitMQ,
Postgres, and MinIO testcontainers.  The parsing and indexing services are
replaced by in-process stub HTTP servers — the goal is to verify the task
chain plumbing (broker serialisation, chain wiring, MinIO handoff, cleanup),
not the services themselves.

All assertions are side-effect based:
  - Did the stubs receive the expected requests?
  - Was the MinIO intermediate object cleaned up after success?
  - Was the namespace forwarded correctly to the indexing service?
"""

from __future__ import annotations

import json
import uuid

import pytest
from minio import Minio
from testcontainers.core.container import DockerContainer

from tests.backend.controller.worker.integration.conftest import (
    StubServer,
    upload_file,
    wait_until_minio_empty,
    wait_until_stub_called,
)


def _dispatch_delete_document(dispatch_app, s3_bucket: str, namespace_id: uuid.UUID):
    return dispatch_app.send_task(
        "tasks.ingest",
        kwargs={
            "s3": {"bucket": s3_bucket, "object": "test.md"},
            "source": {"path": "test.md", "content_type": "text/markdown"},
            "upload_action": "DELETE",
            "info": {"namespace_id": str(namespace_id)},
        },
        queue="artemis.ingestion.fetch-and-parse",
        routing_key="fetch-and-parse",
    )


def _dispatch_delete_namespace(dispatch_app, namespace_id: uuid.UUID):
    return dispatch_app.send_task(
        "tasks.delete_namespace",
        kwargs={"namespace_id": str(namespace_id)},
        queue="artemis.ingestion.index",
        routing_key="index",
    )


def _dispatch_ingest(dispatch_app, s3_bucket: str, namespace_id: uuid.UUID):
    return dispatch_app.send_task(
        "tasks.ingest",
        kwargs={
            "s3": {"bucket": s3_bucket, "object": "test.md"},
            "source": {"path": "test.md", "content_type": "text/markdown"},
            "upload_action": "CREATE",
            "info": {"namespace_id": str(namespace_id)},
        },
        queue="artemis.ingestion.fetch-and-parse",
        routing_key="fetch-and-parse",
    )


@pytest.mark.integration
class TestWorkerStartup:
    """Verify worker lifecycle hooks run correctly on startup."""

    def test_parsed_chunks_bucket_created_on_startup(
        self,
        minio_client: Minio,
        worker_container: DockerContainer,
    ) -> None:
        """The worker must create the 'parsed-chunks' MinIO bucket via the
        ``worker_ready`` signal handler before accepting any tasks.

        This guards against the latent bug where ``ParsedChunkStore.save()``
        would fail silently if the bucket did not exist.
        """
        assert minio_client.bucket_exists("parsed-chunks")


@pytest.mark.integration
class TestFetchAndParseIndexChain:
    def test_happy_path_chain_calls_both_stubs(
        self,
        dispatch_app,
        parsing_stub: StubServer,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """Full chain: both the parsing and indexing stubs must be hit exactly once."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        assert len(parsing_stub.requests) == 1
        assert len(indexing_stub.requests) == 1

    def test_parsing_stub_receives_correct_filename(
        self,
        dispatch_app,
        parsing_stub: StubServer,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The filename must be forwarded in the multipart body to the parsing service."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        assert b"test.md" in parsing_stub.requests[0]["body"]

    def test_indexing_stub_receives_correct_namespace(
        self,
        dispatch_app,
        parsing_stub: StubServer,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """
        The namespace UUID must be forwarded as
        a query param to the indexing service.
        """
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        assert str(namespace_id) in indexing_stub.requests[0]["path"]

    def test_indexing_stub_receives_parsed_chunks(
        self,
        dispatch_app,
        parsing_stub: StubServer,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The indexing service must receive the chunks returned by the parsing stub."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        body = json.loads(indexing_stub.requests[0]["body"])
        assert len(body) == 2
        assert body[0]["page_content"] == "hello world"
        assert body[0]["type"] == "text"
        assert body[1]["type"] == "table"

    def test_intermediate_minio_object_cleaned_up_after_success(
        self,
        dispatch_app,
        parsing_stub: StubServer,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """After successful indexing the parsed-chunks MinIO object must be deleted."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)
        wait_until_minio_empty(minio_client, "parsed-chunks", timeout=30)

    # TODO: test that the MinIO intermediate object is PRESERVED when indexing fails.
    #   - Push a 500 response to indexing_stub so the task fails after all retries.
    #   - Assert the "parsed-chunks" object is still present in MinIO.
    #   - This validates the dead-letter replay design contract.
    #   - Note: with autoretry_for + retry_backoff this test will be very slow unless
    #     max_retries is patched to 0 for the test worker.


@pytest.mark.integration
class TestDeleteDocumentTask:
    """Tier 3 tests for the delete_document task.

    ``ingest`` with ``upload_action=DELETE`` dispatches ``delete_document`` to
    the index queue.  ``delete_document`` calls ``DELETE /ingest?namespace=...
    &source=...`` on the indexing service.  No file download or parsing occurs.
    """

    def test_delete_document_hits_indexing_stub(
        self,
        dispatch_app,
        parsing_stub: StubServer,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """
        The indexing stub must receive exactly one DELETE request;
        parsing is never called.
        """
        _dispatch_delete_document(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_requests = [r for r in indexing_stub.requests if r["method"] == "DELETE"]
        assert len(delete_requests) == 1
        assert len(parsing_stub.requests) == 0

    def test_delete_document_stub_receives_correct_namespace(
        self,
        dispatch_app,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The namespace UUID must appear as a query param in the DELETE path."""
        _dispatch_delete_document(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert str(namespace_id) in delete_req["path"]

    def test_delete_document_stub_receives_correct_source(
        self,
        dispatch_app,
        indexing_stub: StubServer,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The source file path must appear as a query param in the DELETE path."""
        _dispatch_delete_document(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert "source=test.md" in delete_req["path"]


@pytest.mark.integration
class TestDeleteNamespaceTask:
    """Tier 3 tests for the delete_namespace task.

    ``delete_namespace`` is dispatched directly to the index queue and calls
    ``DELETE /ingest?namespace=...`` (no ``source`` param) on the indexing
    service, triggering a full namespace wipe.
    """

    def test_delete_namespace_hits_indexing_stub(
        self,
        dispatch_app,
        indexing_stub: StubServer,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The indexing stub must receive exactly one DELETE request."""
        _dispatch_delete_namespace(dispatch_app, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_requests = [r for r in indexing_stub.requests if r["method"] == "DELETE"]
        assert len(delete_requests) == 1

    def test_delete_namespace_stub_receives_correct_namespace(
        self,
        dispatch_app,
        indexing_stub: StubServer,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The namespace UUID must appear as a query param in the DELETE path."""
        _dispatch_delete_namespace(dispatch_app, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert str(namespace_id) in delete_req["path"]

    def test_delete_namespace_stub_receives_no_source(
        self,
        dispatch_app,
        indexing_stub: StubServer,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """No ``source`` query param must be present — this is a namespace-level wipe."""
        _dispatch_delete_namespace(dispatch_app, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert "source=" not in delete_req["path"]
