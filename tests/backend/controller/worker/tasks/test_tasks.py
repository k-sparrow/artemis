"""Unit tests for the Celery task orchestration logic.

Tasks are called via `.run()` which invokes the underlying function directly,
bypassing all Celery machinery (broker, result backend, retries).  All external
dependencies are patched so no infrastructure is required.

The conftest at the parent level sets `database_create_tables_at_setup=False`
before tasks.py is first imported, preventing DatabaseBackend from opening a
real DB connection during module initialisation.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest

from src.backend.controller.lib.schemas import (
    BlobRef,
    IngestionInfo,
    SourceDetails,
    S3Details,
    UploadAction,
)
from src.backend.controller.worker.exceptions import EmptyObjectError

# ----- module under test (imported after conftest patches the app) ----------
from src.backend.controller.worker.tasks import (
    delete_document,
    delete_namespace,
    fetch_and_parse,
    index,
    ingest,
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

    ``ingest`` is a pure routing task: it inspects ``upload_action`` and
    dispatches the appropriate downstream task(s) without touching any
    infrastructure itself.

    Key design constraints verified here:
    - CREATE/UPDATE must dispatch a ``fetch_and_parse → index`` chain, with
      Pydantic models serialised to plain dicts/strings so they survive the
      JSON round-trip through the broker (regression: EncodeError was raised
      when Pydantic models were passed directly).
    - DELETE/AUTO_DELETE must dispatch ``delete_document`` (not the chain) and
      must pass ``source`` as a dict so the broker can serialise it.
    """

    def test_create_dispatches_chain(self) -> None:
        """CREATE action must dispatch the fetch_and_parse → index chain.

        Also verifies the serialisation boundary: ``fetch_and_parse`` must
        receive plain dicts/strings, not Pydantic model instances, so that
        kombu can JSON-encode them across the broker.
        """
        mock_chain_result = MagicMock()
        mock_chain_result.id = "chain-abc"

        with patch("src.backend.controller.worker.tasks.chain") as mock_chain:
            mock_chain.return_value.apply_async.return_value = mock_chain_result

            result = ingest.run(
                s3=_S3,
                source=_SOURCE,
                upload_action=UploadAction.CREATE,
                info=_INFO,
            )

        assert "chain_id" in result
        mock_chain.assert_called_once()
        # First task: fetch_and_parse must receive serialisable args, with the
        # contract task_id propagated as the trailing arg.
        first_sig = mock_chain.call_args[0][0]
        assert first_sig.args[:4] == (
            _S3.model_dump(),
            _SOURCE.model_dump(),
            str(_NAMESPACE_ID),
            None,  # group_id — None when not set on IngestionInfo
        )
        propagated_task_id = first_sig.args[4]
        assert isinstance(propagated_task_id, str) and propagated_task_id
        # Second task: index must receive upload_action + source + s3 dicts for the
        # JDBC sink result, plus the SAME contract task_id propagated to fetch_and_parse.
        second_sig = mock_chain.call_args[0][1]
        assert second_sig.args == (
            str(_NAMESPACE_ID),
            "CREATE",  # upload_action
            None,  # group_id
            _SOURCE.model_dump(mode="json"),
            _S3.model_dump(mode="json"),
            propagated_task_id,  # contract id is the same across both subtasks
        )

    def test_update_dispatches_chain(self) -> None:
        """UPDATE action must follow the same chain path as CREATE."""
        mock_chain_result = MagicMock()
        mock_chain_result.id = "chain-def"

        with patch("src.backend.controller.worker.tasks.chain") as mock_chain:
            mock_chain.return_value.apply_async.return_value = mock_chain_result

            result = ingest.run(
                s3=_S3,
                source=_SOURCE,
                upload_action=UploadAction.UPDATE,
                info=_INFO,
            )

        assert "chain_id" in result
        mock_chain.assert_called_once()

    def test_delete_dispatches_delete_document(self) -> None:
        """DELETE action must dispatch ``delete_document`` with serialised kwargs.

        ``source`` must be a dict (``model_dump()``) and ``namespace_id`` a
        string so the broker can JSON-encode them.
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
                }
            )


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
        Successful deletion must return an IngestionResult dict for the CDC pipeline.
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
# fetch_and_parse
# ---------------------------------------------------------------------------


class TestFetchAndParse:
    """Tests for ``fetch_and_parse`` — hands parsing a source ``BlobRef`` and
    returns the artifact ``BlobRef`` (claim-check; no download, no chunk store)."""

    def _run(self, mock_parse: MagicMock) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_parsing_service", mock_parse
        ):
            return fetch_and_parse.run(_S3, _SOURCE, _NAMESPACE_ID)

    def test_returns_artifact_ref_dict(self) -> None:
        """The task returns the artifact BlobRef (serialised) for the chain."""
        result = self._run(MagicMock(return_value=_ARTIFACT_REF))
        assert result == {"bucket": _ARTIFACT_REF.bucket, "key": _ARTIFACT_REF.key}

    def test_parsing_called_with_source_ref_from_s3(self) -> None:
        """The input S3 coordinates are forwarded as a source BlobRef."""
        mock_parse = MagicMock(return_value=_ARTIFACT_REF)
        self._run(mock_parse)

        source_ref = mock_parse.call_args[1]["source_ref"]
        assert source_ref.bucket == _S3.bucket
        assert source_ref.key == _S3.object
        assert mock_parse.call_args[1]["source"].source == "test.md"

    def test_empty_object_raises_empty_object_error(self) -> None:
        """size=0 in the contract must raise EmptyObjectError before calling parsing."""
        with pytest.raises(EmptyObjectError):
            fetch_and_parse.run(_S3_EMPTY, _SOURCE, _NAMESPACE_ID)


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
        """The task returns a structured IngestionResult dict for the JDBC sink."""
        result = self._run(MagicMock(), MagicMock())
        assert result["object"]["id"] == str(_OBJ_ID)
        assert result["object"]["source"] == _SOURCE.source
        assert result["object"]["scope"]["namespace_id"] == str(_NAMESPACE_ID)
        assert result["object"]["scope"]["group_id"] is None
        assert result["object"]["properties"]["object_type"] == _SOURCE.object_type
        assert result["object"]["properties"]["content_type"] == _SOURCE.content_type
        assert result["object"]["properties"]["size_bytes"] == _S3.size
        assert result["indexing"]["num_added"] == _UPSERT_RESULT["num_added"]
        assert result["indexing"]["ids"] == _UPSERT_RESULT["ids"]
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
