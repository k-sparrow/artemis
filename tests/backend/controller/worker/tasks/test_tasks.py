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
from src.backend.controller.worker.tasks import fetch_and_parse, index, ingest

_NAMESPACE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_NAMESPACE_STR = str(_NAMESPACE_ID)

_S3 = S3Details(bucket="docs", object="files/test.md")
_SOURCE = SourceDetails(path="test.md", content_type="text/markdown")
_INFO = IngestionInfo(namespace_id=_NAMESPACE_ID)

# dict forms used for asserting what the task passes through to sub-tasks
_S3_DICT = _S3.model_dump()
_SOURCE_DICT = _SOURCE.model_dump()

_CHUNKS: List[ParsedChunk] = [
    ParsedChunk(page_content="hello", source="test.md", type=ChunkType.TEXT),
    ParsedChunk(page_content="| a |", source="test.md", type=ChunkType.TABLE),
]

_UPSERT_RESULT = {"num_added": 2, "num_updated": 0, "num_skipped": 0, "ids": ["x", "y"]}


# ---------------------------------------------------------------------------
# ingest — entry point routing
# ---------------------------------------------------------------------------


class TestIngest:
    def test_create_dispatches_chain(self) -> None:
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
        # First task in the chain must be fetch_and_parse with the right args
        first_sig = mock_chain.call_args[0][0]
        assert first_sig.args == (_S3_DICT, _SOURCE_DICT, _NAMESPACE_STR)

    def test_update_dispatches_chain(self) -> None:
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

    def test_delete_returns_skip_status(self) -> None:
        result = ingest.run(
            s3=_S3,
            source=_SOURCE,
            upload_action=UploadAction.DELETE,
            info=_INFO,
        )

        assert result == {"status": "delete_skipped"}

    def test_auto_delete_returns_skip_status(self) -> None:
        result = ingest.run(
            s3=_S3,
            source=_SOURCE,
            upload_action=UploadAction.AUTO_DELETE,
            info=_INFO,
        )

        assert result == {"status": "delete_skipped"}


# ---------------------------------------------------------------------------
# fetch_and_parse
# ---------------------------------------------------------------------------


class TestFetchAndParse:
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
            return fetch_and_parse.run(_S3_DICT, _SOURCE_DICT, _NAMESPACE_STR)

    def test_returns_minio_key(self) -> None:
        result = self._run(MagicMock(), MagicMock(), MagicMock())
        assert result == "parsed-chunks/task-123.json"

    def test_fetch_called_with_s3_details(self) -> None:
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
            fetch_and_parse.run(_S3_DICT, _SOURCE_DICT, _NAMESPACE_STR)

        s3_arg = mock_fetch.call_args[0][1]
        assert s3_arg.bucket == "docs"
        assert s3_arg.object == "files/test.md"

    def test_parsing_called_with_file_bytes_and_source(self) -> None:
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
            fetch_and_parse.run(_S3_DICT, _SOURCE_DICT, _NAMESPACE_STR)

        assert mock_parse.call_args[1]["file_bytes"] == b"file bytes"
        assert mock_parse.call_args[1]["source"].path == "test.md"

    def test_chunks_saved_to_store(self) -> None:
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
            fetch_and_parse.run(_S3_DICT, _SOURCE_DICT, _NAMESPACE_STR)

        saved_chunks = mock_store_instance.save.call_args[0][0]
        assert saved_chunks == _CHUNKS


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------


class TestIndex:
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
            return index.run(self._KEY, _NAMESPACE_STR)

    def test_returns_upsert_result(self) -> None:
        result = self._run(MagicMock(), MagicMock())
        assert result == _UPSERT_RESULT

    def test_loads_chunks_from_store(self) -> None:
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
            index.run(self._KEY, _NAMESPACE_STR)

        mock_store_instance.load.assert_called_once_with(self._KEY)

    def test_indexing_called_with_correct_namespace(self) -> None:
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
            index.run(self._KEY, _NAMESPACE_STR)

        assert mock_index_svc.call_args[1]["namespace_id"] == _NAMESPACE_ID

    def test_minio_key_deleted_after_success(self) -> None:
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
            index.run(self._KEY, _NAMESPACE_STR)

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
                index.run(self._KEY, _NAMESPACE_STR)

        mock_store_instance.delete.assert_not_called()
