"""Unit tests for the Celery task orchestration logic.

Tasks are called via `.run()` which invokes the underlying function directly,
bypassing all Celery machinery (broker, retries). Celery's own result
backend is now the stock `db+` recipe (see celery.py) — nothing in this
codebase blocks on `.get()`, so it's irrelevant to these tests either way.
All external dependencies (including ingestion_status DB writes) are
patched so no infrastructure is required.
"""

from __future__ import annotations

import uuid
from unittest.mock import ANY, MagicMock, patch

import httpx
import pybreaker
import pytest
from celery.exceptions import Retry

from src.backend.controller.lib.schemas import (
    BlobRef,
    IngestionInfo,
    SourceDetails,
    S3Details,
    UploadAction,
)

from src.backend.controller.worker.tasks import (
    OutboxTask,
    TerminalOutboxTask,
    delete_document,
    delete_namespace,
    index,
    ingest,
    parse,
)

_NAMESPACE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_OBJ_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab")

_S3 = S3Details(bucket="docs", object="files/test.md", size=10)
_S3_EMPTY = S3Details(bucket="docs", object="files/empty.md", size=0)
_SOURCE = SourceDetails(
    source="test.md",
    content_type="text/markdown",
    obj_id=_OBJ_ID,
    object_type="file",
)
_INFO = IngestionInfo(namespace_id=_NAMESPACE_ID)

_ARTIFACT_REF = BlobRef(bucket="parsed-chunks", key=f"parse/{_OBJ_ID}.json")

_UPSERT_RESULT = {"num_added": 2, "num_updated": 0, "num_skipped": 0, "ids": ["x", "y"]}


# ---------------------------------------------------------------------------
# ingest — entry point routing
# ---------------------------------------------------------------------------


class TestIngest:
    """Tests for ``ingest`` — the Kafka HTTP Sink entry point.

    ``ingest`` inspects ``upload_action`` and dispatches the appropriate
    downstream task(s) — but it does touch infrastructure directly now: it's
    the sole INSERT for this run's ingestion_status row (see
    backend/outbox.py), since it's the only point in the pipeline holding
    every identity field at once. That DB write is mocked via the autouse
    fixture below for every test in this class except the one that asserts
    on it directly.

    Key design constraints verified here:
    - CREATE/UPDATE must dispatch ``parse`` standalone (NOT chained with
      ``index`` — index is part of the tail chain that whichever of
      poll_parse / advance_from_callback wins the parse_stage_state claim
      builds and dispatches explicitly; see tasks.py's module docstring),
      with Pydantic models serialised to plain dicts/strings so they survive
      the JSON round-trip through the broker (regression: EncodeError was
      raised when Pydantic models were passed directly).
    - DELETE/AUTO_DELETE must dispatch ``delete_document`` (not the chain) and
      must pass ``source`` as a dict so the broker can serialise it.
    """

    @pytest.fixture(autouse=True)
    def _mock_outbox_row(self):
        with (
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
            patch(
                "src.backend.controller.worker.tasks.outbox.create_status_row"
            ) as mock_create,
        ):
            yield mock_create

    def test_writes_ingestion_status_row_before_dispatch(
        self, _mock_outbox_row
    ) -> None:
        with patch.object(parse, "apply_async", return_value=MagicMock(id="parse-abc")):
            ingest.run(
                s3=_S3, source=_SOURCE, upload_action=UploadAction.CREATE, info=_INFO
            )

        _mock_outbox_row.assert_called_once()
        call_kwargs = _mock_outbox_row.call_args.kwargs
        assert call_kwargs["namespace_id"] == _NAMESPACE_ID
        assert call_kwargs["obj_id"] == _OBJ_ID
        assert call_kwargs["source"] == _SOURCE.source
        assert call_kwargs["object_type"] == _SOURCE.object_type
        assert call_kwargs["content_type"] == _SOURCE.content_type
        assert call_kwargs["size_bytes"] == _S3.size
        assert call_kwargs["operation"] == "CREATE"

    def test_create_dispatches_parse_standalone(self) -> None:
        """CREATE action must dispatch ``parse`` alone, not chained with index.

        Also verifies the serialisation boundary: ``parse`` must
        receive plain dicts/strings, not Pydantic model instances, so that
        kombu can JSON-encode them across the broker.
        """
        mock_result = MagicMock()
        mock_result.id = "parse-abc"

        with patch.object(parse, "apply_async", return_value=mock_result) as mock_apply:
            result = ingest.run(
                s3=_S3,
                source=_SOURCE,
                upload_action=UploadAction.CREATE,
                info=_INFO,
            )

        assert result == {"task_id": "parse-abc"}
        mock_apply.assert_called_once()
        # parse must receive serialisable args. Identity is passed by KEYWORD
        # so OutboxTask.on_failure can recover it from kwargs; the
        # contract task_id is propagated alongside.
        call_kwargs = mock_apply.call_args.kwargs
        propagated_task_id = call_kwargs["kwargs"]["task_id"]
        assert isinstance(propagated_task_id, str) and propagated_task_id
        assert call_kwargs["args"] == (_S3.model_dump(),)
        assert call_kwargs["kwargs"] == {
            "source": _SOURCE.model_dump(mode="json"),
            "namespace_id": str(_NAMESPACE_ID),
            "group_id": None,  # None when not set on IngestionInfo
            "task_id": propagated_task_id,
            "operation": "CREATE",
        }

    def test_update_dispatches_parse_standalone(self) -> None:
        """UPDATE action must follow the same dispatch path as CREATE."""
        mock_result = MagicMock()
        mock_result.id = "parse-def"

        with patch.object(parse, "apply_async", return_value=mock_result) as mock_apply:
            result = ingest.run(
                s3=_S3,
                source=_SOURCE,
                upload_action=UploadAction.UPDATE,
                info=_INFO,
            )

        assert result == {"task_id": "parse-def"}
        mock_apply.assert_called_once()

    def test_delete_dispatches_delete_document(self) -> None:
        """DELETE action must dispatch ``delete_document`` with serialised kwargs.

        ``source`` must be a dict (``model_dump()``) and ``namespace_id`` a
        string so the broker can JSON-encode them.  The contract ``task_id`` is
        threaded through so ``ingestion_status`` is keyed by the id storage
        returned (``ANY`` here — a fresh UUID since the direct ``.run()`` has no
        request id).
        """
        mock_task_result = MagicMock()
        mock_task_result.id = "task-del-1"

        with patch.object(
            delete_document, "apply_async", return_value=mock_task_result
        ) as mock_apply:
            result = ingest.run(
                s3=_S3,
                source=_SOURCE,
                upload_action=UploadAction.DELETE,
                info=_INFO,
            )

            assert result == {"task_id": "task-del-1"}
            mock_apply.assert_called_once_with(
                kwargs={
                    "source": _SOURCE.model_dump(),
                    "namespace_id": str(_NAMESPACE_ID),
                    "task_id": ANY,
                    "operation": "DELETE",
                }
            )

    def test_auto_delete_dispatches_delete_document(self) -> None:
        """AUTO_DELETE (TTL-expired object) must follow the same path as DELETE."""
        mock_task_result = MagicMock()
        mock_task_result.id = "task-del-2"

        with patch.object(
            delete_document, "apply_async", return_value=mock_task_result
        ) as mock_apply:
            result = ingest.run(
                s3=_S3,
                source=_SOURCE,
                upload_action=UploadAction.AUTO_DELETE,
                info=_INFO,
            )

            assert result == {"task_id": "task-del-2"}
            mock_apply.assert_called_once_with(
                kwargs={
                    "source": _SOURCE.model_dump(),
                    "namespace_id": str(_NAMESPACE_ID),
                    "task_id": ANY,
                    "operation": "AUTO_DELETE",
                }
            )


# ---------------------------------------------------------------------------
# OutboxTask.before_start / on_failure / on_success
# ---------------------------------------------------------------------------


class TestOutboxTaskOnFailure:
    """``on_failure`` marks ``ingestion_status`` FAILURE for the contract
    ``task_id`` recovered from this task's own invocation kwargs — see
    backend/outbox.py."""

    def _invoke(self, exc: Exception, kwargs: dict) -> MagicMock:
        """Call ``on_failure`` with a mock task; return the ``mark_failure`` mock.

        ``on_failure`` delegates to ``record_failure`` (shared with
        ``advance_from_callback``) — bind the REAL method onto the mock so
        ``self.record_failure(...)`` inside ``on_failure`` still exercises it,
        instead of silently no-op-ing on an auto-created Mock attribute.
        """
        task = MagicMock()
        task.name = "tasks.index"
        task.request.id = "subtask-C-id"
        task.record_failure = OutboxTask.record_failure.__get__(task, OutboxTask)
        with (
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
            patch(
                "src.backend.controller.worker.tasks.outbox.mark_failure"
            ) as mock_mark_failure,
        ):
            OutboxTask.on_failure(task, exc, "subtask-C-id", (), kwargs, einfo=None)
        return mock_mark_failure

    def test_marks_failure_keyed_by_contract_task_id(self) -> None:
        mock_mark_failure = self._invoke(ValueError("boom"), {"task_id": "contract-A"})
        mock_mark_failure.assert_called_once()
        call_kwargs = mock_mark_failure.call_args.kwargs
        assert call_kwargs["task_id"] == "contract-A"
        assert call_kwargs["failure_reason"] == "ValueError: boom"

    def test_skips_when_contract_task_id_missing(self) -> None:
        mock_mark_failure = self._invoke(ValueError("x"), {})
        mock_mark_failure.assert_not_called()

    def test_failing_tasks_use_the_base(self) -> None:
        assert isinstance(index, TerminalOutboxTask)
        assert isinstance(delete_document, TerminalOutboxTask)
        assert isinstance(index, OutboxTask)  # TerminalOutboxTask IS-A OutboxTask
        # ingest (writes its own row explicitly) and delete_namespace (out of
        # ingestion_status scope entirely) do NOT use the outbox base.
        assert not isinstance(ingest, OutboxTask)
        assert not isinstance(delete_namespace, OutboxTask)


class TestTerminalOutboxTaskOnSuccess:
    """``on_success`` marks ``ingestion_status`` SUCCESS — only reachable on
    tasks using ``TerminalOutboxTask`` (``index``, ``delete_document``)."""

    def test_marks_success_keyed_by_contract_task_id(self) -> None:
        task = MagicMock()
        with (
            patch(
                "src.backend.controller.worker.tasks.SessionLocal",
                return_value=MagicMock(),
            ),
            patch(
                "src.backend.controller.worker.tasks.outbox.mark_success"
            ) as mock_mark_success,
        ):
            TerminalOutboxTask.on_success(
                task, {"ok": True}, "subtask-C-id", (), {"task_id": "contract-A"}
            )
        mock_mark_success.assert_called_once()
        assert mock_mark_success.call_args.kwargs["task_id"] == "contract-A"

    def test_skips_when_contract_task_id_missing(self) -> None:
        task = MagicMock()
        with patch(
            "src.backend.controller.worker.tasks.outbox.mark_success"
        ) as mock_mark_success:
            TerminalOutboxTask.on_success(task, {"ok": True}, "subtask-C-id", (), {})
        mock_mark_success.assert_not_called()


# ---------------------------------------------------------------------------
# delete_document
# ---------------------------------------------------------------------------


class TestDeleteDocument:
    """Tests for the ``delete_document`` task.

    ``delete_document`` receives a ``SourceDetails`` and a namespace UUID and
    calls ``call_delete_service`` with ``obj_id`` and namespace so the indexing
    service can remove the file's chunks from the vectorstore and record manager.
    """

    def _run(self, mock_delete_svc: MagicMock, source: SourceDetails = _SOURCE) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_delete_service", mock_delete_svc
        ):
            return delete_document.run(source=source, namespace_id=_NAMESPACE_ID)

    def test_returns_deleted_status(self) -> None:
        """
        Successful deletion must return a structured IngestionResult dict.
        """
        result = self._run(MagicMock())
        assert result["object"]["id"] == str(_OBJ_ID)
        assert result["object"]["scope"]["namespace_id"] == str(_NAMESPACE_ID)
        assert result["operation"] == "DELETE"

    def test_call_delete_service_called_with_correct_args(self) -> None:
        """The indexing service must be called with obj_id and namespace."""
        mock_delete_svc = MagicMock()
        self._run(mock_delete_svc)
        assert mock_delete_svc.call_args[1]["namespace_id"] == _NAMESPACE_ID
        assert mock_delete_svc.call_args[1]["obj_id"] == str(_OBJ_ID)


# ---------------------------------------------------------------------------
# delete_namespace
# ---------------------------------------------------------------------------


class TestDeleteNamespace:
    """Tests for the ``delete_namespace`` task.

    ``delete_namespace`` wipes every document belonging to a namespace by
    calling ``call_delete_service`` with no ``source`` argument.  The indexing
    service interprets the absence of ``source`` as a full namespace wipe,
    delegating to ``pipeline.aprocess([])`` which triggers LangChain's
    ``scoped_full`` cleanup over an empty document list — deleting all tracked
    keys for the namespace from both the vectorstore and the record manager.
    """

    def _run(self, mock_delete_svc: MagicMock) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_delete_service", mock_delete_svc
        ):
            return delete_namespace.run(namespace_id=_NAMESPACE_ID)

    def test_returns_deleted_namespace_status(self) -> None:
        """Successful wipe must return a status dict with the namespace id."""
        result = self._run(MagicMock())
        assert result == {
            "status": "deleted_namespace",
            "namespace_id": str(_NAMESPACE_ID),
        }

    def test_call_delete_service_called_without_source(self) -> None:
        """The service call must carry the namespace but no ``source`` kwarg —
        the absence of ``source`` is the signal for a full namespace wipe."""
        mock_delete_svc = MagicMock()
        self._run(mock_delete_svc)
        call_kwargs = mock_delete_svc.call_args[1]
        assert call_kwargs["namespace_id"] == _NAMESPACE_ID
        assert "source" not in call_kwargs

    def test_call_delete_service_called_with_correct_namespace(self) -> None:
        """The correct namespace UUID must be forwarded to the indexing service."""
        mock_delete_svc = MagicMock()
        self._run(mock_delete_svc)
        assert mock_delete_svc.call_args[1]["namespace_id"] == _NAMESPACE_ID


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------


class TestIndex:
    """Tests for ``index`` — reads the artifact via the indexing service and
    deletes it on success (left in place on failure for dead-letter replay)."""

    def _run(self, mock_index_svc: MagicMock, mock_store: MagicMock, **kwargs) -> dict:
        mock_index_svc.return_value = _UPSERT_RESULT
        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.MinioBlobStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            return index.run(
                _ARTIFACT_REF, _NAMESPACE_ID, "CREATE", source=_SOURCE, s3=_S3, **kwargs
            )

    def test_returns_ingestion_result_with_obj_id(self) -> None:
        """The task returns a structured IngestionResult dict."""
        result = self._run(MagicMock(), MagicMock())
        assert result["object"]["id"] == str(_OBJ_ID)
        assert result["object"]["source"] == _SOURCE.source
        assert result["object"]["scope"]["namespace_id"] == str(_NAMESPACE_ID)
        assert result["object"]["scope"]["group_id"] is None
        assert result["object"]["properties"]["object_type"] == _SOURCE.object_type
        assert result["object"]["properties"]["content_type"] == _SOURCE.content_type
        assert result["object"]["properties"]["size_bytes"] == _S3.size
        assert result["indexing"]["num_added"] == _UPSERT_RESULT["num_added"]
        assert result["operation"] == "CREATE"

    def test_returns_group_id_when_provided(self) -> None:
        group_id = str(uuid.uuid4())
        result = self._run(MagicMock(), MagicMock(), group_id=group_id)
        assert result["object"]["scope"]["group_id"] == group_id

    def test_indexing_called_with_artifact_ref_and_namespace(self) -> None:
        mock_index_svc = MagicMock(return_value=_UPSERT_RESULT)
        self._run(mock_index_svc, MagicMock())
        assert mock_index_svc.call_args[1]["artifact_ref"] == _ARTIFACT_REF
        assert mock_index_svc.call_args[1]["namespace_id"] == _NAMESPACE_ID

    def test_artifact_deleted_after_success(self) -> None:
        """The artifact is deleted from its bucket after a successful index."""
        mock_store = MagicMock()
        self._run(MagicMock(), mock_store)
        # MinioBlobStore(client, ref.bucket).delete(ref.key)
        assert mock_store.call_args[0][1] == _ARTIFACT_REF.bucket
        mock_store.return_value.delete.assert_called_once_with(_ARTIFACT_REF.key)

    def test_artifact_not_deleted_on_indexing_failure(self) -> None:
        """On failure the artifact must be left for dead-letter replay."""
        mock_index_svc = MagicMock(side_effect=RuntimeError("indexing service down"))
        mock_store = MagicMock()
        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.MinioBlobStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(RuntimeError):
                index.run(
                    _ARTIFACT_REF, _NAMESPACE_ID, "CREATE", source=_SOURCE, s3=_S3
                )

        mock_store.return_value.delete.assert_not_called()


# ---------------------------------------------------------------------------
# Retry strategy
# ---------------------------------------------------------------------------


def _make_http_error(status_code: int) -> httpx.HTTPStatusError:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    return httpx.HTTPStatusError(
        str(status_code), request=MagicMock(), response=response
    )


class TestIndexRetry:
    """Retry strategy for index."""

    def _run_expecting_retry(self, exc: Exception) -> MagicMock:
        mock_retry = MagicMock(side_effect=Retry())
        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                side_effect=exc,
            ),
            patch.object(index, "retry", mock_retry),
        ):
            with pytest.raises(Retry):
                index.run(
                    _ARTIFACT_REF, _NAMESPACE_ID, "CREATE", source=_SOURCE, s3=_S3
                )
        return mock_retry

    def test_5xx_retried_with_exponential_backoff(self) -> None:
        mock_retry = self._run_expecting_retry(_make_http_error(503))
        mock_retry.assert_called_once()
        kw = mock_retry.call_args.kwargs
        assert kw["max_retries"] == 20
        assert 1 <= kw["countdown"] <= 2

    def test_circuit_breaker_retried_after_reset_window(self) -> None:
        mock_retry = self._run_expecting_retry(
            pybreaker.CircuitBreakerError("Timeout not elapsed yet")
        )
        mock_retry.assert_called_once()
        kw = mock_retry.call_args.kwargs
        assert kw["max_retries"] == 20
        assert 125 <= kw["countdown"] <= 155


class TestDeleteDocumentRetry:
    """Retry strategy for delete_document."""

    def _run_expecting_retry(self, exc: Exception) -> MagicMock:
        mock_retry = MagicMock(side_effect=Retry())
        with (
            patch(
                "src.backend.controller.worker.tasks.call_delete_service",
                side_effect=exc,
            ),
            patch.object(delete_document, "retry", mock_retry),
        ):
            with pytest.raises(Retry):
                delete_document.run(source=_SOURCE, namespace_id=_NAMESPACE_ID)
        return mock_retry

    def test_5xx_retried_with_exponential_backoff(self) -> None:
        mock_retry = self._run_expecting_retry(_make_http_error(503))
        mock_retry.assert_called_once()
        kw = mock_retry.call_args.kwargs
        assert kw["max_retries"] == 20
        assert 1 <= kw["countdown"] <= 2

    def test_circuit_breaker_retried_after_reset_window(self) -> None:
        mock_retry = self._run_expecting_retry(
            pybreaker.CircuitBreakerError("Timeout not elapsed yet")
        )
        mock_retry.assert_called_once()
        kw = mock_retry.call_args.kwargs
        assert kw["max_retries"] == 20
        assert 125 <= kw["countdown"] <= 155


class TestDeleteNamespaceRetry:
    """Retry strategy for delete_namespace."""

    def _run_expecting_retry(self, exc: Exception) -> MagicMock:
        mock_retry = MagicMock(side_effect=Retry())
        with (
            patch(
                "src.backend.controller.worker.tasks.call_delete_service",
                side_effect=exc,
            ),
            patch.object(delete_namespace, "retry", mock_retry),
        ):
            with pytest.raises(Retry):
                delete_namespace.run(namespace_id=_NAMESPACE_ID)
        return mock_retry

    def test_5xx_retried_with_exponential_backoff(self) -> None:
        mock_retry = self._run_expecting_retry(_make_http_error(503))
        mock_retry.assert_called_once()
        kw = mock_retry.call_args.kwargs
        assert kw["max_retries"] == 20
        assert 1 <= kw["countdown"] <= 2

    def test_circuit_breaker_retried_after_reset_window(self) -> None:
        mock_retry = self._run_expecting_retry(
            pybreaker.CircuitBreakerError("Timeout not elapsed yet")
        )
        mock_retry.assert_called_once()
        kw = mock_retry.call_args.kwargs
        assert kw["max_retries"] == 20
        assert 125 <= kw["countdown"] <= 155
