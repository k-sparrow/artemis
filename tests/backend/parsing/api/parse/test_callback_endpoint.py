"""Contract/schema tests for POST /v1/parse/callback/{obj_id}.

Exercises this service's own request parsing against docling-serve's real
ProgressCallbackRequest shapes (document_completed success/failure/
partial_success/skipped, plus the set_num_docs/update_processed no-op
shapes) and the enqueue-vs-outcome HTTP status contract described in the
endpoint's own docstring: 200 means "handed off to Celery", nothing about
whether the document actually converted.

No Celery broker here — get_celery_producer is overridden with a mock, since
this layer only cares about what gets enqueued, not whether it's delivered.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.dependencies import get_celery_producer
from src.backend.parsing.api.main import app

OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")
OBJ_ID_STR = str(OBJ_ID)
DOCLING_TASK_ID = "conv-task-1"


def _document_completed(status: str, *, error: str | None = None) -> dict:
    document: dict = {"source": "doc.pdf", "status": status}
    if error is not None:
        document["error"] = error
    return {
        "task_id": DOCLING_TASK_ID,
        "progress": {
            "kind": "document_completed",
            "document": document,
            "total_processed": 1,
        },
    }


def _set_num_docs() -> dict:
    return {
        "task_id": DOCLING_TASK_ID,
        "progress": {"kind": "set_num_docs", "num_docs": 3},
    }


def _update_processed() -> dict:
    return {
        "task_id": DOCLING_TASK_ID,
        "progress": {
            "kind": "update_processed",
            "num_processed": 1,
            "num_succeeded": 1,
            "num_partially_succeeded": 0,
            "num_failed": 0,
            "docs": [{"source": "doc.pdf", "status": "success"}],
        },
    }


@pytest.fixture
def producer() -> MagicMock:
    return MagicMock()


@pytest.fixture
def client(producer: MagicMock) -> TestClient:
    app.dependency_overrides[get_celery_producer] = lambda: producer
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


def _post(client: TestClient, body: dict):
    return client.post(f"/v1/parse/callback/{OBJ_ID_STR}", json=body)


class TestDocumentCompleted:
    def test_success_enqueues_success_outcome_and_acks(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        resp = _post(client, _document_completed("success"))

        assert resp.status_code == 200
        assert resp.json() == {"status": "ack"}
        producer.send_task.assert_called_once_with(
            "tasks.advance_from_callback",
            args=[OBJ_ID_STR],
            kwargs={
                "stage": "convert",
                "docling_task_id": DOCLING_TASK_ID,
                "outcome": "success",
                "error_message": None,
            },
            queue="artemis.ingestion.parse",
            exchange=settings.EXCHANGE_NAME,
            routing_key="parse",
        )

    def test_failure_enqueues_failure_outcome_with_error(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        resp = _post(client, _document_completed("failure", error="model exploded"))

        assert resp.status_code == 200
        kwargs = producer.send_task.call_args.kwargs
        assert kwargs["kwargs"]["outcome"] == "failure"
        assert kwargs["kwargs"]["error_message"] == "model exploded"

    def test_partial_success_maps_to_failure_outcome(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        """Mirrors DoclingParseClient.get_status: some page slices failed
        server-side — treated as a failure so the retry re-submits the whole
        document rather than proceeding with a truncated result."""
        resp = _post(
            client, _document_completed("partial_success", error="page 3 failed")
        )

        assert resp.status_code == 200
        kwargs = producer.send_task.call_args.kwargs["kwargs"]
        assert kwargs["outcome"] == "failure"
        assert kwargs["error_message"] == "partial_success: page 3 failed"

    def test_partial_success_without_error_detail_still_maps_to_failure(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        resp = _post(client, _document_completed("partial_success"))

        assert resp.status_code == 200
        kwargs = producer.send_task.call_args.kwargs["kwargs"]
        assert kwargs["outcome"] == "failure"
        assert kwargs["error_message"] == "partial_success"

    def test_skipped_maps_to_failure_outcome(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        """Unexpected in this flow (always a fresh single-document submit) —
        fails loudly rather than silently treating it as success."""
        resp = _post(client, _document_completed("skipped"))

        assert resp.status_code == 200
        kwargs = producer.send_task.call_args.kwargs["kwargs"]
        assert kwargs["outcome"] == "failure"
        assert kwargs["error_message"] == "status=skipped"

    def test_obj_id_from_path_not_from_callback_body(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        """The claim key is the path param (this service's own trusted
        identity for the document), never anything docling-serve echoes."""
        _post(client, _document_completed("success"))

        call_args = producer.send_task.call_args
        assert call_args.kwargs["args"] == [OBJ_ID_STR]


class TestProgressNoOpBranches:
    """set_num_docs / update_processed carry no terminal outcome — ack only,
    nothing enqueued yet."""

    def test_set_num_docs_acks_without_enqueueing(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        resp = _post(client, _set_num_docs())

        assert resp.status_code == 200
        assert resp.json() == {"status": "ack"}
        producer.send_task.assert_not_called()

    def test_update_processed_acks_without_enqueueing(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        resp = _post(client, _update_processed())

        assert resp.status_code == 200
        assert resp.json() == {"status": "ack"}
        producer.send_task.assert_not_called()


class TestEnqueueFailure:
    def test_send_task_failure_returns_503_not_2xx(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        """The one case where a non-2xx is correct: docling-serve's
        CallbackInvoker retries redelivering the callback on non-2xx, which is
        exactly the useful behavior when the broker itself is unreachable —
        unlike a 4xx/5xx from the document's own conversion outcome, which
        would be pointless to have docling-serve retry."""
        producer.send_task.side_effect = Exception("broker unreachable")

        resp = _post(client, _document_completed("success"))

        assert resp.status_code == 503

    def test_send_task_failure_does_not_ack(
        self, client: TestClient, producer: MagicMock
    ) -> None:
        producer.send_task.side_effect = Exception("broker unreachable")

        resp = _post(client, _document_completed("success"))

        assert resp.json() != {"status": "ack"}
