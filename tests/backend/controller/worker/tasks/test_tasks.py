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
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from src.lib.core.ingestion.types import ChunkType, ParsedChunk
from src.backend.controller.lib.schemas import (
    IngestionInfo,
    SourceDetails,
    S3Details,
    UploadAction,
)

# ----- module under test (imported after conftest patches the app) ----------
from src.backend.controller.worker.tasks import (
    delete_document,
    delete_namespace,
    fetch_and_parse,
    index,
    ingest,
)

_NAMESPACE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

_S3 = S3Details(bucket="docs", object="files/test.md")
_SOURCE = SourceDetails(path="test.md", content_type="text/markdown")
_INFO = IngestionInfo(namespace_id=_NAMESPACE_ID)

_CHUNKS: List[ParsedChunk] = [
    ParsedChunk(page_content="hello", source="test.md", type=ChunkType.TEXT),
    ParsedChunk(page_content="| a |", source="test.md", type=ChunkType.TABLE),
]

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
        # First task in the chain must be fetch_and_parse with serialisable args
        first_sig = mock_chain.call_args[0][0]
        assert first_sig.args == (
            _S3.model_dump(),
            _SOURCE.model_dump(),
            str(_NAMESPACE_ID),
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


_SOURCE_NO_PATH = SourceDetails(path=None, content_type="text/markdown")


class TestDeleteDocument:
    """Tests for the ``delete_document`` task.

    ``delete_document`` receives a ``SourceDetails`` and a namespace UUID and
    calls ``call_delete_service`` with the source path and namespace so the
    indexing service can remove the file's chunks from the vectorstore and
    record manager.

    The early-exit guard (``source.path is None``) exists because MinIO object
    events can arrive without a meaningful path when the original upload had no
    key — in that case there is nothing to delete and the task returns a skip
    status without calling the indexing service.
    """

    def _run(self, mock_delete_svc: MagicMock, source: SourceDetails = _SOURCE) -> dict:
        with patch(
            "src.backend.controller.worker.tasks.call_delete_service", mock_delete_svc
        ):
            return delete_document.run(source=source, namespace_id=_NAMESPACE_ID)

    def test_returns_deleted_status(self) -> None:
        """Successful deletion must return a status dict with source and namespace."""
        result = self._run(MagicMock())
        assert result == {
            "status": "deleted",
            "source": "test.md",
            "namespace_id": str(_NAMESPACE_ID),
        }

    def test_call_delete_service_called_with_correct_args(self) -> None:
        """The indexing service must be called with the resolved path and namespace."""
        mock_delete_svc = MagicMock()
        self._run(mock_delete_svc)
        assert mock_delete_svc.call_args[1]["namespace_id"] == _NAMESPACE_ID
        assert mock_delete_svc.call_args[1]["source"] == "test.md"

    def test_skips_when_source_path_is_none(self) -> None:
        """When ``source.path`` is None the task must return early without calling
        the indexing service — there is no object key to delete."""
        mock_delete_svc = MagicMock()
        result = self._run(mock_delete_svc, source=_SOURCE_NO_PATH)
        assert result == {"status": "skipped", "reason": "source_path_missing"}
        mock_delete_svc.assert_not_called()


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
    """Tests for the ``fetch_and_parse`` task (Task 1 of the ingestion chain).

    ``fetch_and_parse`` downloads the raw file from MinIO, sends it to the
    parsing service, persists the returned chunks to a temporary MinIO object,
    and returns the object key for ``index`` to consume.

    The task never passes raw bytes between tasks — only the short MinIO key
    travels over the broker, keeping the Postgres result backend lean.
    """

    def _run(
        self,
        mock_fetch: MagicMock,
        mock_parse: MagicMock,
        mock_store: MagicMock,
    ) -> str:
        """Run fetch_and_parse.run() with all external deps patched."""
        mock_fetch.return_value = b"file bytes"
        mock_parse.return_value = _CHUNKS
        mock_store_instance = MagicMock()
        mock_store_instance.save.return_value = "parsed-chunks/task-123.json"
        mock_store.return_value = mock_store_instance

        with (
            patch("src.backend.controller.worker.tasks.fetch_from_s3", mock_fetch),
            patch(
                "src.backend.controller.worker.tasks.call_parsing_service", mock_parse
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            return fetch_and_parse.run(_S3, _SOURCE, _NAMESPACE_ID)

    def test_returns_minio_key(self) -> None:
        """The task must return the MinIO object key produced by ParsedChunkStore."""
        result = self._run(MagicMock(), MagicMock(), MagicMock())
        assert result == "parsed-chunks/task-123.json"

    def test_fetch_called_with_s3_details(self) -> None:
        """The S3 coordinates must be forwarded unchanged to ``fetch_from_s3``."""
        mock_fetch = MagicMock(return_value=b"bytes")
        mock_parse = MagicMock(return_value=_CHUNKS)
        mock_store = MagicMock()
        mock_store.return_value.save.return_value = "key"

        with (
            patch("src.backend.controller.worker.tasks.fetch_from_s3", mock_fetch),
            patch(
                "src.backend.controller.worker.tasks.call_parsing_service", mock_parse
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            fetch_and_parse.run(_S3, _SOURCE, _NAMESPACE_ID)

        s3_arg = mock_fetch.call_args[0][1]
        assert s3_arg.bucket == "docs"
        assert s3_arg.object == "files/test.md"

    def test_parsing_called_with_file_bytes_and_source(self) -> None:
        """The raw bytes and source metadata must be forwarded to the parsing service."""
        mock_fetch = MagicMock(return_value=b"file bytes")
        mock_parse = MagicMock(return_value=_CHUNKS)
        mock_store = MagicMock()
        mock_store.return_value.save.return_value = "key"

        with (
            patch("src.backend.controller.worker.tasks.fetch_from_s3", mock_fetch),
            patch(
                "src.backend.controller.worker.tasks.call_parsing_service", mock_parse
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            fetch_and_parse.run(_S3, _SOURCE, _NAMESPACE_ID)

        assert mock_parse.call_args[1]["file_bytes"] == b"file bytes"
        assert mock_parse.call_args[1]["source"].path == "test.md"

    def test_chunks_saved_to_store(self) -> None:
        """The chunks returned by the parsing service must be persisted to MinIO."""
        mock_fetch = MagicMock(return_value=b"bytes")
        mock_parse = MagicMock(return_value=_CHUNKS)
        mock_store = MagicMock()
        mock_store_instance = mock_store.return_value
        mock_store_instance.save.return_value = "key"

        with (
            patch("src.backend.controller.worker.tasks.fetch_from_s3", mock_fetch),
            patch(
                "src.backend.controller.worker.tasks.call_parsing_service", mock_parse
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            fetch_and_parse.run(_S3, _SOURCE, _NAMESPACE_ID)

        saved_chunks = mock_store_instance.save.call_args[0][0]
        assert saved_chunks == _CHUNKS


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------


class TestIndex:
    """Tests for the ``index`` task (Task 2 of the ingestion chain).

    ``index`` receives the MinIO key from ``fetch_and_parse``, loads the
    chunks, POSTs them to the indexing service, and deletes the MinIO object
    on success.  On failure the object is left in MinIO so it can be replayed
    via the dead-letter queue.
    """

    _KEY = "parsed-chunks/task-123.json"

    def _run(
        self,
        mock_index_svc: MagicMock,
        mock_store: MagicMock,
        chunks: List[ParsedChunk] = _CHUNKS,
    ) -> dict:
        mock_store_instance = MagicMock()
        mock_store_instance.load.return_value = chunks
        mock_store.return_value = mock_store_instance
        mock_index_svc.return_value = _UPSERT_RESULT

        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            return index.run(self._KEY, _NAMESPACE_ID)

    def test_returns_upsert_result(self) -> None:
        """The task must return the upsert result dict from the indexing service."""
        result = self._run(MagicMock(), MagicMock())
        assert result == _UPSERT_RESULT

    def test_loads_chunks_from_store(self) -> None:
        """
        Chunks must be loaded from MinIO using the
        key passed by ``fetch_and_parse``.
        """
        mock_index_svc = MagicMock(return_value=_UPSERT_RESULT)
        mock_store = MagicMock()
        mock_store_instance = mock_store.return_value
        mock_store_instance.load.return_value = _CHUNKS

        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            index.run(self._KEY, _NAMESPACE_ID)

        mock_store_instance.load.assert_called_once_with(self._KEY)

    def test_indexing_called_with_correct_namespace(self) -> None:
        """The namespace UUID must be forwarded to the indexing service."""
        mock_index_svc = MagicMock(return_value=_UPSERT_RESULT)
        mock_store = MagicMock()
        mock_store.return_value.load.return_value = _CHUNKS

        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            index.run(self._KEY, _NAMESPACE_ID)

        assert mock_index_svc.call_args[1]["namespace_id"] == _NAMESPACE_ID

    def test_minio_key_deleted_after_success(self) -> None:
        """The intermediate MinIO object must be cleaned up after successful indexing."""
        mock_index_svc = MagicMock(return_value=_UPSERT_RESULT)
        mock_store = MagicMock()
        mock_store_instance = mock_store.return_value
        mock_store_instance.load.return_value = _CHUNKS

        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            index.run(self._KEY, _NAMESPACE_ID)

        mock_store_instance.delete.assert_called_once_with(self._KEY)

    def test_minio_key_not_deleted_on_indexing_failure(self) -> None:
        """On failure the object must be left in MinIO for dead-letter replay."""
        mock_index_svc = MagicMock(side_effect=RuntimeError("indexing service down"))
        mock_store = MagicMock()
        mock_store_instance = mock_store.return_value
        mock_store_instance.load.return_value = _CHUNKS

        with (
            patch(
                "src.backend.controller.worker.tasks.call_indexing_service",
                mock_index_svc,
            ),
            patch("src.backend.controller.worker.tasks.ParsedChunkStore", mock_store),
            patch(
                "src.backend.controller.worker.tasks.get_s3_client",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(RuntimeError):
                index.run(self._KEY, _NAMESPACE_ID)

        mock_store_instance.delete.assert_not_called()
