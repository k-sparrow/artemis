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
    WireMockStub,
    upload_file,
    wait_until_minio_empty,
    wait_until_stub_called,
    wait_until_stub_called_n_times,
)


def _source_dict(namespace_id: uuid.UUID, filename: str = "test.md") -> dict:
    return {
        "source": filename,
        "content_type": "text/markdown",
        "obj_id": str(uuid.uuid5(namespace_id, filename)),
        "object_type": "file",
    }


def _dispatch_delete_document(dispatch_app, s3_bucket: str, namespace_id: uuid.UUID):
    return dispatch_app.send_task(
        "tasks.ingest",
        kwargs={
            "s3": {"bucket": s3_bucket, "object": "test.md", "size": 1},
            "source": _source_dict(namespace_id),
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
            "s3": {"bucket": s3_bucket, "object": "test.md", "size": 1},
            "source": _source_dict(namespace_id),
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
        """The worker creates the 'parsed-chunks' MinIO bucket via the
        ``worker_ready`` signal handler on startup.

        The parsing service owns and writes this bucket; the worker only deletes
        the artifact from it after a successful index. The startup hook ensures
        the bucket exists so the cleanup-delete never targets a missing bucket.
        """
        assert minio_client.bucket_exists("parsed-chunks")


@pytest.mark.integration
class TestFetchAndParseIndexChain:
    def test_happy_path_chain_calls_both_stubs(
        self,
        dispatch_app,
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """Full chain: both the parsing and indexing stubs must be hit exactly once."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        # Async chain calls 5 parsing endpoints: submit + status×2 + resolve + finalize
        assert len(parsing_stub.requests) == 5
        assert len(indexing_stub.requests) == 1

    def test_parsing_stub_receives_correct_filename(
        self,
        dispatch_app,
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The filename must be forwarded as a form field to the parsing service."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        submit_req = next(
            r for r in parsing_stub.requests if r["path"].endswith("/submit")
        )
        assert b"test.md" in submit_req["body"]

    def test_indexing_stub_receives_correct_namespace(
        self,
        dispatch_app,
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
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

    def test_indexing_stub_receives_artifact_ref(
        self,
        dispatch_app,
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """Claim-check: the indexing service receives the artifact BlobRef the
        parsing stub returned — not the chunk payload."""
        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60)

        body = json.loads(indexing_stub.requests[0]["body"])
        assert body["artifact_ref"]["bucket"] == "parsed-chunks"
        assert body["artifact_ref"]["key"].startswith("parse/")

    def test_intermediate_minio_object_cleaned_up_after_success(
        self,
        dispatch_app,
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
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
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
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
        indexing_stub: WireMockStub,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """The namespace UUID must appear as a query param in the DELETE path."""
        _dispatch_delete_document(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert str(namespace_id) in delete_req["path"]

    def test_delete_document_stub_receives_correct_obj_id(
        self,
        dispatch_app,
        indexing_stub: WireMockStub,
        s3_source_bucket: str,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """
        The obj_id derived from (namespace_id, filename) must appear
        as a query param.
        """
        _dispatch_delete_document(dispatch_app, s3_source_bucket, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        expected_obj_id = str(uuid.uuid5(namespace_id, "test.md"))
        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert f"obj_id={expected_obj_id}" in delete_req["path"]


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
        indexing_stub: WireMockStub,
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
        indexing_stub: WireMockStub,
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
        indexing_stub: WireMockStub,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """No ``source`` query param must be present — this is a namespace-level wipe."""
        _dispatch_delete_namespace(dispatch_app, namespace_id)
        wait_until_stub_called(indexing_stub, timeout=60, method="DELETE")

        delete_req = next(r for r in indexing_stub.requests if r["method"] == "DELETE")
        assert "source=" not in delete_req["path"]


@pytest.mark.integration
class TestRetryBehavior:
    """Verify that tasks retry on transient HTTP failures and eventually succeed.

    The indexing stub's push_response() acts as a scenario: one queued 503
    followed by the default 200.  The worker must retry tasks.index, hit the
    stub a second time, and succeed.  tasks.fetch_and_parse must not be retried
    — only tasks.index autoretries on Exception.
    """

    def test_index_retries_on_transient_indexing_failure(
        self,
        dispatch_app,
        parsing_stub: WireMockStub,
        indexing_stub: WireMockStub,
        s3_source_bucket: str,
        minio_client: Minio,
        namespace_id: uuid.UUID,
        worker_container: DockerContainer,
    ) -> None:
        """tasks.index retries once after a 503 from the indexing service.

        The stub returns 503 on the first call (triggering autoretry) then
        falls back to its default 200.  After the retry succeeds:
          - indexing stub received exactly 2 POST calls (attempt + retry)
          - parsing stub received exactly 1 POST call (fetch_and_parse not retried)
          - the parsed-chunks MinIO object is cleaned up (indexing eventually succeeded)
        """
        indexing_stub.push_response(503, {"detail": "service unavailable"})

        upload_file(minio_client, s3_source_bucket, "test.md")
        _dispatch_ingest(dispatch_app, s3_source_bucket, namespace_id)

        # Wait for the retry cycle: indexing stub must be called twice.
        # First call → 503 (autoretry), second call → 200 (success).
        # retry_backoff=True means the first retry fires after ~2s.
        wait_until_stub_called_n_times(indexing_stub, n=2, timeout=60)

        # Parsing chain: submit + resolve + finalize = 3 POST calls (status uses GET)
        assert sum(1 for r in parsing_stub.requests if r["method"] == "POST") == 3
        assert sum(1 for r in indexing_stub.requests if r["method"] == "POST") == 2

        # Successful retry means the index task completed — cleanup must follow.
        wait_until_minio_empty(minio_client, "parsed-chunks", timeout=30)
