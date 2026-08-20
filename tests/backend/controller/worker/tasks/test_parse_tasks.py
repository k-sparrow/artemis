"""Unit tests for the async-parse Celery tasks: resolve_parse, submit_chunk,
poll_parse, poll_chunk.

Tasks are exercised via ``.run()`` to bypass broker machinery. All external
calls are patched; no infrastructure required.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import httpx
import pybreaker
import pytest
from celery.exceptions import MaxRetriesExceededError, Retry  # noqa: F401
from sqlalchemy.exc import DatabaseError

from src.backend.controller.lib.schemas import S3Details, SourceDetails
from src.backend.controller.worker.backend.claims import ResumeContext
from src.backend.controller.worker.exceptions import EmptyObjectError
from src.backend.controller.worker.tasks import (
    DocumentChunkingError,
    DocumentConversionError,
    advance_from_callback,
    poll_chunk,
    poll_parse,
    resolve_parse,
    submit_chunk,
    submit_parse,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SUBMIT_RESULT = {
    "parsing_task_id": "conv-task-1",
    "obj_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
}

_RESOLVE_RESULT = {
    "obj_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
}

_CHUNK_SUBMIT_RESULT = {
    "chunking_task_id": "chunk-task-1",
    "obj_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
}

_BLOB_REF = {
    "bucket": "parsed-chunks",
    "key": "parse/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json",
}

_KWARGS = {
    "namespace_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab",
    "task_id": "contract-task-id",
    "group_id": None,
    "operation": "CREATE",
}

# poll_parse alone also takes obj_id (threaded from parse(), not read from
# submit_result — see tasks.py's poll_parse docstring for why).
_POLL_PARSE_KWARGS = {**_KWARGS, "obj_id": _SUBMIT_RESULT["obj_id"]}

_SOURCE = {
    "source": "doc.pdf",
    "content_type": "application/pdf",
    "obj_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "object_type": "file",
}

_FAKE_RESUME_CONTEXT = ResumeContext(
    namespace_id=_KWARGS["namespace_id"],
    group_id=_KWARGS["group_id"],
    task_id=_KWARGS["task_id"],
    operation=_KWARGS["operation"],
    mode="source",
    source=_SOURCE,
    s3={"bucket": "docs", "object": "files/doc.pdf", "size": 10},
)


def _make_status(status: str) -> dict:
    return {
        "status": status,
        "num_processed": None,
        "num_total": None,
        "error_message": None,
    }


def _make_http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://parsing/v1/parse/status/x")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


# ---------------------------------------------------------------------------
# submit_parse
# ---------------------------------------------------------------------------

_SUBMIT_PARSE_NAMESPACE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab")
_SUBMIT_PARSE_OBJ_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_SUBMIT_PARSE_SOURCE = SourceDetails(
    source="doc.pdf",
    content_type="application/pdf",
    obj_id=_SUBMIT_PARSE_OBJ_ID,
    object_type="file",
)
_SUBMIT_PARSE_S3 = S3Details(bucket="docs", object="files/doc.pdf", size=10)
_SUBMIT_PARSE_S3_EMPTY = S3Details(bucket="docs", object="files/empty.pdf", size=0)
_SUBMIT_PARSE_KWARGS = {
    "source": _SUBMIT_PARSE_SOURCE,
    "namespace_id": _SUBMIT_PARSE_NAMESPACE_ID,
    "group_id": None,
    "task_id": "contract-task-id",
    "operation": "CREATE",
}
_SUBMIT_RESULT_STUB = {
    "parsing_task_id": "conv-task-1",
    "obj_id": str(_SUBMIT_PARSE_OBJ_ID),
    "mode": "source",
}


class TestSubmitParse:
    """submit_parse's claim-row write (new, this feature) plus its
    pre-existing HTTP/circuit-breaker retry behavior (previously untested at
    this layer — there was no TestSubmitParse class at all before this)."""

    def _run(self, mock_submit: MagicMock, *, s3: S3Details = _SUBMIT_PARSE_S3):
        with (
            patch("src.backend.controller.worker.tasks.call_parse_submit", mock_submit),
            patch(
                "src.backend.controller.worker.tasks.claims.ensure_stage_row"
            ) as mock_ensure,
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            result = submit_parse.run(s3, **_SUBMIT_PARSE_KWARGS)
            return result, mock_ensure

    def test_writes_claim_row_before_calling_docling_serve(self) -> None:
        call_order: list[str] = []
        mock_submit = MagicMock(
            side_effect=lambda **_: (
                call_order.append("call_parse_submit"),
                dict(_SUBMIT_RESULT_STUB),
            )[1]
        )
        with (
            patch("src.backend.controller.worker.tasks.call_parse_submit", mock_submit),
            patch(
                "src.backend.controller.worker.tasks.claims.ensure_stage_row",
                side_effect=lambda *a, **k: call_order.append("ensure_stage_row"),
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            submit_parse.run(_SUBMIT_PARSE_S3, **_SUBMIT_PARSE_KWARGS)

        assert call_order == ["ensure_stage_row", "call_parse_submit"]

    def test_claim_row_keyed_by_obj_id_with_correct_resume_context(self) -> None:
        mock_submit = MagicMock(return_value=dict(_SUBMIT_RESULT_STUB))
        _, mock_ensure = self._run(mock_submit)

        mock_ensure.assert_called_once()
        call_kwargs = mock_ensure.call_args.kwargs
        assert call_kwargs["obj_id"] == str(_SUBMIT_PARSE_OBJ_ID)
        assert call_kwargs["stage"] == "convert"
        resume_context = call_kwargs["resume_context"]
        assert resume_context.namespace_id == str(_SUBMIT_PARSE_NAMESPACE_ID)
        assert resume_context.task_id == "contract-task-id"
        assert resume_context.operation == "CREATE"
        assert resume_context.mode == "source"
        assert resume_context.source.obj_id == _SUBMIT_PARSE_OBJ_ID
        assert resume_context.s3.bucket == "docs"

    def test_empty_object_raises_without_writing_claim_row(self) -> None:
        with (
            patch(
                "src.backend.controller.worker.tasks.call_parse_submit"
            ) as mock_submit,
            patch(
                "src.backend.controller.worker.tasks.claims.ensure_stage_row"
            ) as mock_ensure,
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(EmptyObjectError):
                submit_parse.run(_SUBMIT_PARSE_S3_EMPTY, **_SUBMIT_PARSE_KWARGS)

        mock_ensure.assert_not_called()
        mock_submit.assert_not_called()

    def test_success_returns_submit_result_with_timestamp(self) -> None:
        mock_submit = MagicMock(return_value=dict(_SUBMIT_RESULT_STUB))
        result, _ = self._run(mock_submit)
        assert result["parsing_task_id"] == "conv-task-1"
        assert "submitted_at" in result

    def test_claim_row_db_error_schedules_retry(self) -> None:
        """self.retry(exc=exc, ...) re-raises the original exception (not a
        bare Retry) — same pattern as this file's other exc=-based retry
        tests (test_circuit_breaker_schedules_retry etc.); in production
        Celery schedules a genuine retry instead of propagating."""
        with (
            patch("src.backend.controller.worker.tasks.call_parse_submit"),
            patch(
                "src.backend.controller.worker.tasks.claims.ensure_stage_row",
                side_effect=DatabaseError("stmt", {}, Exception("connection refused")),
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(DatabaseError):
                submit_parse.run(_SUBMIT_PARSE_S3, **_SUBMIT_PARSE_KWARGS)

    def test_circuit_breaker_schedules_retry(self) -> None:
        mock_submit = MagicMock(side_effect=pybreaker.CircuitBreakerError("open"))
        with pytest.raises(pybreaker.CircuitBreakerError):
            self._run(mock_submit)

    def test_5xx_schedules_retry(self) -> None:
        mock_submit = MagicMock(side_effect=_make_http_error(503))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock_submit)
        assert exc_info.value.response.status_code == 503

    def test_4xx_reraises_permanently(self) -> None:
        mock_submit = MagicMock(side_effect=_make_http_error(422))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock_submit)
        assert exc_info.value.response.status_code == 422


# ---------------------------------------------------------------------------
# poll_parse
# ---------------------------------------------------------------------------


class TestPollParse:
    """poll_parse's own HTTP-polling logic. The parse_stage_state claim it
    races advance_from_callback for is stubbed here — defaults to "wins the
    claim" so the pre-existing success/failure assertions still hold; claims
    atomicity itself has its own dedicated tests (backend/claims.py), and the
    "loses the claim" no-op path is covered explicitly below.
    """

    def _run(self, mock_status: MagicMock, *, won_claim: bool = True) -> dict:
        resume_context = _FAKE_RESUME_CONTEXT if won_claim else None
        with (
            patch("src.backend.controller.worker.tasks.call_parse_status", mock_status),
            patch(
                "src.backend.controller.worker.tasks.claims.backfill_docling_task_id"
            ),
            patch(
                "src.backend.controller.worker.tasks.claims.try_claim",
                return_value=resume_context,
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
            patch("src.backend.controller.worker.tasks._build_and_dispatch_tail_chain"),
        ):
            return poll_parse.run(_SUBMIT_RESULT, **_POLL_PARSE_KWARGS)

    def test_processing_raises_retry(self) -> None:
        """'processing' status must schedule a retry (poll again later)."""
        mock = MagicMock(return_value=_make_status("processing"))
        with pytest.raises(Retry):
            self._run(mock)

    def test_failure_raises_document_conversion_error(self) -> None:
        """Terminal 'failure' must raise DocumentConversionError — no retry."""
        mock = MagicMock(return_value=_make_status("failure"))
        with pytest.raises(DocumentConversionError):
            self._run(mock)

    def test_success_returns_submit_result_unchanged(self) -> None:
        """'success' must return the SubmitResult dict so resolve_parse can use it."""
        mock = MagicMock(return_value=_make_status("success"))
        result = self._run(mock)
        assert result == _SUBMIT_RESULT

    def test_lost_claim_on_success_is_a_silent_noop(self) -> None:
        """Losing the claim (callback path already won it) must not raise or
        dispatch — just return quietly."""
        mock = MagicMock(return_value=_make_status("success"))
        result = self._run(mock, won_claim=False)
        assert result is None

    def test_lost_claim_on_failure_is_a_silent_noop(self) -> None:
        """Losing the claim on a failure outcome must not raise either — the
        callback path already recorded (or will record) the FAILURE row."""
        mock = MagicMock(return_value=_make_status("failure"))
        result = self._run(mock, won_claim=False)
        assert result is None

    def test_circuit_breaker_schedules_retry(self) -> None:
        """CircuitBreakerError must be re-raised (not swallowed) so the broker retries.

        In eager mode self.retry(exc=e) propagates the original exception;
        in production Celery schedules a retry — both sides are correct.
        """
        mock = MagicMock(side_effect=pybreaker.CircuitBreakerError("open"))
        with pytest.raises(pybreaker.CircuitBreakerError):
            self._run(mock)

    def test_5xx_http_error_schedules_retry(self) -> None:
        """5xx from the parsing service must be re-raised for broker-level retry."""
        mock = MagicMock(side_effect=_make_http_error(503))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 503

    def test_4xx_http_error_reraises_permanently(self) -> None:
        """4xx errors are permanent caller mistakes — must not be retried."""
        mock = MagicMock(side_effect=_make_http_error(422))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 422

    def test_backfill_db_error_schedules_retry(self) -> None:
        """The docling_task_id backfill (this task's first line) is its own
        DB touchpoint, separate from the try_claim calls exercised above —
        a transient error there must retry too, not propagate as a false
        conversion failure. Uses poll_parse's own 1440 budget, not
        submit_parse/advance_from_callback's 20 — a real bug this session
        (a smaller budget here would raise MaxRetriesExceededError once
        self.request.retries, driven by ordinary 'processing' polls, exceeds
        it) — see _retry_on_db_error's docstring."""
        mock_status = MagicMock(return_value=_make_status("success"))
        with (
            patch("src.backend.controller.worker.tasks.call_parse_status", mock_status),
            patch(
                "src.backend.controller.worker.tasks.claims.backfill_docling_task_id",
                side_effect=DatabaseError("stmt", {}, Exception("connection refused")),
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(DatabaseError):
                poll_parse.run(_SUBMIT_RESULT, **_POLL_PARSE_KWARGS)
        mock_status.assert_not_called()

    def test_status_called_with_correct_task_id(self) -> None:
        """
        The conversion task_id from SubmitResult must be forwarded to call_parse_status.
        """
        mock = MagicMock(return_value=_make_status("success"))
        self._run(mock)
        assert mock.call_args[1]["task_id"] == _SUBMIT_RESULT["parsing_task_id"]


# ---------------------------------------------------------------------------
# advance_from_callback
# ---------------------------------------------------------------------------


class TestAdvanceFromCallback:
    """advance_from_callback's own claim-arbitration logic. claims.try_claim
    itself is stubbed here — its real atomicity is covered by the per-service
    integration suite (tests/backend/controller/worker/backend/test_claims.py)
    against a real Postgres, not mocked out.
    """

    @contextmanager
    def _patched(self, *, claim_result):
        with (
            patch(
                "src.backend.controller.worker.tasks.claims.try_claim",
                return_value=claim_result,
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
            patch(
                "src.backend.controller.worker.tasks._build_and_dispatch_tail_chain"
            ) as mock_dispatch,
            patch.object(advance_from_callback, "record_failure") as mock_record,
        ):
            yield mock_dispatch, mock_record

    def test_loses_claim_is_a_silent_noop(self) -> None:
        """The other path (poll_parse) already handled or will handle this —
        no dispatch, no failure record, just a quiet status."""
        with self._patched(claim_result=None) as (mock_dispatch, mock_record):
            result = advance_from_callback.run(
                "obj-1",
                stage="convert",
                docling_task_id="conv-task-1",
                outcome="success",
            )

        assert result == {"status": "not_claimed"}
        mock_dispatch.assert_not_called()
        mock_record.assert_not_called()

    def test_wins_claim_on_success_dispatches_tail_chain(self) -> None:
        with self._patched(claim_result=_FAKE_RESUME_CONTEXT) as (
            mock_dispatch,
            mock_record,
        ):
            result = advance_from_callback.run(
                "obj-1",
                stage="convert",
                docling_task_id="conv-task-1",
                outcome="success",
            )

        assert result == {"status": "dispatched"}
        mock_dispatch.assert_called_once()
        submit_result, resume_context_arg = mock_dispatch.call_args[0]
        assert submit_result == {
            "parsing_task_id": "conv-task-1",
            "obj_id": "obj-1",
            "mode": _FAKE_RESUME_CONTEXT.mode,
        }
        assert resume_context_arg is _FAKE_RESUME_CONTEXT
        mock_record.assert_not_called()

    def test_wins_claim_on_failure_records_failure_then_raises(self) -> None:
        """Unlike poll_parse, this task has no chain kwargs of its own — the
        failure branch must call record_failure explicitly, using the claimed
        row's resume_context, since on_failure's automatic kwargs-based
        recovery would silently no-op here (no task_id/namespace_id in this
        task's own kwargs)."""
        with self._patched(claim_result=_FAKE_RESUME_CONTEXT) as (
            mock_dispatch,
            mock_record,
        ):
            with pytest.raises(DocumentConversionError):
                advance_from_callback.run(
                    "obj-1",
                    stage="convert",
                    docling_task_id="conv-task-1",
                    outcome="failure",
                    error_message="docling blew up",
                )

        mock_dispatch.assert_not_called()
        mock_record.assert_called_once()
        call_kwargs = mock_record.call_args.kwargs
        assert call_kwargs["contract_task_id"] == _FAKE_RESUME_CONTEXT.task_id
        assert "docling blew up" in call_kwargs["failure_reason"]

    def test_db_retry_exhaustion_does_not_call_record_failure(self) -> None:
        """A DB outage during the claim itself must fail as a plain Celery
        task WITHOUT ever calling record_failure — per the task's own
        docstring, a DB outage means "we don't know what happened to this
        document", not "conversion failed"; poll_parse remains authoritative.
        ``.apply(retries=20)`` seeds request.retries at this task's own
        max_retries — _retry_on_db_error passes exc=exc to self.retry(), so
        exhaustion re-raises the original DatabaseError (not
        MaxRetriesExceededError, which is only the default with no exc=).
        """
        db_error = DatabaseError("SELECT 1", {}, Exception("connection refused"))
        with (
            patch(
                "src.backend.controller.worker.tasks.claims.try_claim",
                side_effect=db_error,
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
            patch.object(advance_from_callback, "record_failure") as mock_record,
        ):
            with pytest.raises(DatabaseError):
                advance_from_callback.apply(
                    args=("obj-1",),
                    kwargs={
                        "stage": "convert",
                        "docling_task_id": "conv-task-1",
                        "outcome": "success",
                    },
                    retries=20,
                )

        mock_record.assert_not_called()


# ---------------------------------------------------------------------------
# resolve_parse
# ---------------------------------------------------------------------------


class TestResolveParse:
    def _run(self, mock_resolve: MagicMock) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_parse_resolve", mock_resolve
        ):
            return resolve_parse.run(_SUBMIT_RESULT, **_KWARGS)

    def test_success_returns_resolve_result(self) -> None:
        """Result of call_parse_resolve is returned unchanged — no chunk job
        submitted here, that's a fully separate stage (Epic 21)."""
        mock = MagicMock(return_value=_RESOLVE_RESULT)
        result = self._run(mock)
        assert result == _RESOLVE_RESULT

    def test_circuit_breaker_schedules_retry(self) -> None:
        mock = MagicMock(side_effect=pybreaker.CircuitBreakerError("open"))
        with pytest.raises(pybreaker.CircuitBreakerError):
            self._run(mock)

    def test_5xx_schedules_retry(self) -> None:
        mock = MagicMock(side_effect=_make_http_error(503))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 503

    def test_4xx_reraises_permanently(self) -> None:
        mock = MagicMock(side_effect=_make_http_error(422))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 422

    def test_called_with_submit_result(self) -> None:
        mock = MagicMock(return_value=_RESOLVE_RESULT)
        self._run(mock)
        assert mock.call_args[1]["submit_result"] == _SUBMIT_RESULT


# ---------------------------------------------------------------------------
# submit_chunk
# ---------------------------------------------------------------------------


class TestSubmitChunk:
    def _run(self, mock_submit: MagicMock) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_chunk_submit", mock_submit
        ):
            return submit_chunk.run(_RESOLVE_RESULT, **_KWARGS)

    def test_success_returns_chunk_submit_result_with_timestamp(self) -> None:
        mock = MagicMock(return_value=dict(_CHUNK_SUBMIT_RESULT))
        result = self._run(mock)
        assert result["chunking_task_id"] == _CHUNK_SUBMIT_RESULT["chunking_task_id"]
        assert result["obj_id"] == _CHUNK_SUBMIT_RESULT["obj_id"]
        assert "submitted_at" in result

    def test_circuit_breaker_schedules_retry(self) -> None:
        mock = MagicMock(side_effect=pybreaker.CircuitBreakerError("open"))
        with pytest.raises(pybreaker.CircuitBreakerError):
            self._run(mock)

    def test_5xx_schedules_retry(self) -> None:
        mock = MagicMock(side_effect=_make_http_error(503))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 503

    def test_4xx_reraises_permanently(self) -> None:
        mock = MagicMock(side_effect=_make_http_error(422))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 422

    def test_called_with_resolve_result(self) -> None:
        mock = MagicMock(return_value=dict(_CHUNK_SUBMIT_RESULT))
        self._run(mock)
        assert mock.call_args[1]["resolve_result"] == _RESOLVE_RESULT


# ---------------------------------------------------------------------------
# poll_chunk
# ---------------------------------------------------------------------------


class TestPollChunk:
    def _run(
        self,
        mock_status: MagicMock,
        mock_finalize: MagicMock | None = None,
    ) -> dict:
        mock_finalize = mock_finalize or MagicMock(return_value=_BLOB_REF)
        with (
            patch("src.backend.controller.worker.tasks.call_chunk_status", mock_status),
            patch(
                "src.backend.controller.worker.tasks.call_chunk_finalize", mock_finalize
            ),
        ):
            return poll_chunk.run(
                _CHUNK_SUBMIT_RESULT,
                source=_SOURCE,
                **_KWARGS,
            )

    def test_processing_raises_retry(self) -> None:
        """'processing' chunk status must schedule a retry."""
        mock = MagicMock(return_value=_make_status("processing"))
        with pytest.raises(Retry):
            self._run(mock)

    def test_failure_raises_document_chunking_error(self) -> None:
        """Terminal 'failure' during chunking must raise DocumentChunkingError."""
        mock = MagicMock(return_value=_make_status("failure"))
        with pytest.raises(DocumentChunkingError):
            self._run(mock)

    def test_success_returns_blob_ref(self) -> None:
        """Successful chunk + finalize must return the BlobRef dict for index."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(return_value=_BLOB_REF)
        result = self._run(mock_status, mock_finalize)
        assert result == _BLOB_REF

    def test_circuit_breaker_on_status_schedules_retry(self) -> None:
        """CircuitBreakerError during status poll must be re-raised for broker retry."""
        mock = MagicMock(side_effect=pybreaker.CircuitBreakerError("open"))
        with pytest.raises(pybreaker.CircuitBreakerError):
            self._run(mock)

    def test_5xx_on_status_schedules_retry(self) -> None:
        """5xx from status poll must be re-raised for broker retry."""
        mock = MagicMock(side_effect=_make_http_error(503))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 503

    def test_4xx_on_status_reraises_permanently(self) -> None:
        """4xx from status poll must not be retried."""
        mock = MagicMock(side_effect=_make_http_error(404))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock)
        assert exc_info.value.response.status_code == 404

    def test_circuit_breaker_on_finalize_schedules_retry(self) -> None:
        """
        CircuitBreakerError during finalize (after success status)
        must be re-raised for retry.
        """
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(side_effect=pybreaker.CircuitBreakerError("open"))
        with pytest.raises(pybreaker.CircuitBreakerError):
            self._run(mock_status, mock_finalize)

    def test_5xx_on_finalize_schedules_retry(self) -> None:
        """
        5xx from finalize must be re-raised for broker retry
        (distinct from chunking failure).
        """
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(side_effect=_make_http_error(503))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            self._run(mock_status, mock_finalize)
        assert exc_info.value.response.status_code == 503

    def test_finalize_called_with_correct_obj_id(self) -> None:
        """finalize metadata must include the obj_id from ChunkSubmitResult."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(return_value=_BLOB_REF)
        self._run(mock_status, mock_finalize)
        assert (
            mock_finalize.call_args[1]["metadata"]["obj_id"]
            == _CHUNK_SUBMIT_RESULT["obj_id"]
        )

    def test_status_called_with_chunking_task_id(self) -> None:
        """Status poll must use the chunking_task_id (not the conversion task_id)."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(return_value=_BLOB_REF)
        self._run(mock_status, mock_finalize)
        assert (
            mock_status.call_args[1]["task_id"]
            == _CHUNK_SUBMIT_RESULT["chunking_task_id"]
        )


# ---------------------------------------------------------------------------
# Retry-budget exhaustion (max_retries=1440)
#
# ``.run()`` bypasses Celery's request machinery entirely: Task.retry() checks
# ``request.called_directly`` first and, when true (which is what ``.run()``
# produces), always raises Retry regardless of max_retries — it never reaches
# the max_retries comparison. ``.apply(retries=N)`` goes through the real eager
# apply path (``called_directly=False``) and seeds ``request.retries``, so it's
# the only way to actually exercise exhaustion here. The exception raised on
# exhaustion depends on how the task called ``self.retry()``:
#   - no ``exc=`` (the "processing" branches) -> Celery raises
#     MaxRetriesExceededError itself.
#   - ``exc=exc`` (the 5xx / circuit-breaker branches) -> Celery re-raises the
#     *original* exception instead, per Task.retry()'s
#     ``if exc: raise_with_context(exc)`` — MaxRetriesExceededError is only the
#     default when no cause exception was supplied.
# Both are legitimate "give up" outcomes; OutboxTask's on_failure
# hook must handle either.
# ---------------------------------------------------------------------------


class TestPollParseRetryExhaustion:
    def test_processing_exhausts_max_retries(self) -> None:
        """The 1440th 'processing' retry attempt must give up, not retry forever."""
        mock = MagicMock(return_value=_make_status("processing"))
        with (
            patch("src.backend.controller.worker.tasks.call_parse_status", mock),
            patch(
                "src.backend.controller.worker.tasks.claims.backfill_docling_task_id"
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(MaxRetriesExceededError):
                poll_parse.apply(
                    args=(_SUBMIT_RESULT,), kwargs=_POLL_PARSE_KWARGS, retries=1440
                )

    def test_5xx_exhausts_max_retries(self) -> None:
        """Transient 5xx errors share the same 1440-retry budget as in-progress polls."""
        mock = MagicMock(side_effect=_make_http_error(503))
        with (
            patch("src.backend.controller.worker.tasks.call_parse_status", mock),
            patch(
                "src.backend.controller.worker.tasks.claims.backfill_docling_task_id"
            ),
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                poll_parse.apply(
                    args=(_SUBMIT_RESULT,), kwargs=_POLL_PARSE_KWARGS, retries=1440
                )
        assert exc_info.value.response.status_code == 503


class TestPollChunkRetryExhaustion:
    def test_processing_exhausts_max_retries(self) -> None:
        """The 1440th 'processing' chunk-status retry must give up, not retry forever."""
        mock = MagicMock(return_value=_make_status("processing"))
        with patch("src.backend.controller.worker.tasks.call_chunk_status", mock):
            with pytest.raises(MaxRetriesExceededError):
                poll_chunk.apply(
                    args=(_CHUNK_SUBMIT_RESULT,),
                    kwargs={**_KWARGS, "source": _SOURCE},
                    retries=1440,
                )

    def test_finalize_5xx_exhausts_max_retries(self) -> None:
        """5xx from finalize (after a successful chunk status) shares the same budget."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(side_effect=_make_http_error(503))
        with (
            patch("src.backend.controller.worker.tasks.call_chunk_status", mock_status),
            patch(
                "src.backend.controller.worker.tasks.call_chunk_finalize", mock_finalize
            ),
        ):
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                poll_chunk.apply(
                    args=(_CHUNK_SUBMIT_RESULT,),
                    kwargs={**_KWARGS, "source": _SOURCE},
                    retries=1440,
                )
        assert exc_info.value.response.status_code == 503
