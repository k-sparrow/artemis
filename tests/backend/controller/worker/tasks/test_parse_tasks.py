"""Unit tests for the async-parse Celery tasks: poll_parse and poll_resolve.

Tasks are exercised via ``.run()`` to bypass broker machinery.  All external
calls are patched; no infrastructure required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pybreaker
import pytest
from celery.exceptions import MaxRetriesExceededError, Retry  # noqa: F401

from src.backend.controller.worker.tasks import (
    DocumentChunkingError,
    DocumentConversionError,
    poll_parse,
    poll_resolve,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SUBMIT_RESULT = {
    "parsing_task_id": "conv-task-1",
    "mode": "single",
    "obj_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "scratch_prefix": None,
    "shard_count": None,
}

_RESOLVE_RESULT = {
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
# poll_parse
# ---------------------------------------------------------------------------


class TestPollParse:
    def _run(self, mock_status: MagicMock) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_parse_status", mock_status
        ):
            return poll_parse.run(_SUBMIT_RESULT, **_KWARGS)

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

    def test_status_called_with_correct_task_id(self) -> None:
        """
        The conversion task_id from SubmitResult must be forwarded to call_parse_status.
        """
        mock = MagicMock(return_value=_make_status("success"))
        self._run(mock)
        assert mock.call_args[1]["task_id"] == _SUBMIT_RESULT["parsing_task_id"]


# ---------------------------------------------------------------------------
# poll_resolve
# ---------------------------------------------------------------------------


class TestPollResolve:
    _SOURCE = {
        "source": "doc.pdf",
        "content_type": "application/pdf",
        "obj_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "object_type": "file",
    }

    def _run(
        self,
        mock_status: MagicMock,
        mock_finalize: MagicMock | None = None,
    ) -> dict:
        mock_finalize = mock_finalize or MagicMock(return_value=_BLOB_REF)
        with (
            patch("src.backend.controller.worker.tasks.call_parse_status", mock_status),
            patch(
                "src.backend.controller.worker.tasks.call_parse_finalize", mock_finalize
            ),
        ):
            return poll_resolve.run(
                _RESOLVE_RESULT,
                source=self._SOURCE,
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
        """finalize metadata must include the obj_id from ResolveResult."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(return_value=_BLOB_REF)
        self._run(mock_status, mock_finalize)
        assert (
            mock_finalize.call_args[1]["metadata"]["obj_id"]
            == _RESOLVE_RESULT["obj_id"]
        )

    def test_status_called_with_chunking_task_id(self) -> None:
        """Status poll must use the chunking_task_id (not the conversion task_id)."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(return_value=_BLOB_REF)
        self._run(mock_status, mock_finalize)
        assert (
            mock_status.call_args[1]["task_id"] == _RESOLVE_RESULT["chunking_task_id"]
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
# Both are legitimate "give up" outcomes; FailureRecordingTask's on_failure
# hook must handle either.
# ---------------------------------------------------------------------------


class TestPollParseRetryExhaustion:
    def test_processing_exhausts_max_retries(self) -> None:
        """The 1440th 'processing' retry attempt must give up, not retry forever."""
        mock = MagicMock(return_value=_make_status("processing"))
        with patch("src.backend.controller.worker.tasks.call_parse_status", mock):
            with pytest.raises(MaxRetriesExceededError):
                poll_parse.apply(args=(_SUBMIT_RESULT,), kwargs=_KWARGS, retries=1440)

    def test_5xx_exhausts_max_retries(self) -> None:
        """Transient 5xx errors share the same 1440-retry budget as in-progress polls."""
        mock = MagicMock(side_effect=_make_http_error(503))
        with patch("src.backend.controller.worker.tasks.call_parse_status", mock):
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                poll_parse.apply(args=(_SUBMIT_RESULT,), kwargs=_KWARGS, retries=1440)
        assert exc_info.value.response.status_code == 503


class TestPollResolveRetryExhaustion:
    _SOURCE = TestPollResolve._SOURCE

    def test_processing_exhausts_max_retries(self) -> None:
        """The 1440th 'processing' chunk-status retry must give up, not retry forever."""
        mock = MagicMock(return_value=_make_status("processing"))
        with patch("src.backend.controller.worker.tasks.call_parse_status", mock):
            with pytest.raises(MaxRetriesExceededError):
                poll_resolve.apply(
                    args=(_RESOLVE_RESULT,),
                    kwargs={**_KWARGS, "source": self._SOURCE},
                    retries=1440,
                )

    def test_finalize_5xx_exhausts_max_retries(self) -> None:
        """5xx from finalize (after a successful chunk status) shares the same budget."""
        mock_status = MagicMock(return_value=_make_status("success"))
        mock_finalize = MagicMock(side_effect=_make_http_error(503))
        with (
            patch("src.backend.controller.worker.tasks.call_parse_status", mock_status),
            patch(
                "src.backend.controller.worker.tasks.call_parse_finalize", mock_finalize
            ),
        ):
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                poll_resolve.apply(
                    args=(_RESOLVE_RESULT,),
                    kwargs={**_KWARGS, "source": self._SOURCE},
                    retries=1440,
                )
        assert exc_info.value.response.status_code == 503
