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
