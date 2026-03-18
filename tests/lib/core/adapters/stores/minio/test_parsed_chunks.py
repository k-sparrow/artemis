"""Unit tests for ParsedChunkStore.

The MinIO client is replaced with a MagicMock — no MinIO container is needed.
Tier 2 integration tests (real MinIO testcontainer) cover the roundtrip with
real I/O; these tests focus on serialisation correctness and call contracts.
"""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, call

import pytest

from src.lib.core.adapters.stores.minio.parsed_chunks import ParsedChunkStore
from src.lib.core.ingestion.types import ChunkType, ParsedChunk

_CHUNKS = [
    ParsedChunk(page_content="hello world", source="test.md", type=ChunkType.TEXT),
    ParsedChunk(page_content="| a | b |", source="report.pdf", type=ChunkType.TABLE),
]

_CHUNKS_JSON = [
    {"page_content": "hello world", "source": "test.md", "type": "text"},
    {"page_content": "| a | b |", "source": "report.pdf", "type": "table"},
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.bucket_exists.return_value = True  # bucket already exists by default
    return client


@pytest.fixture
def store(mock_client: MagicMock) -> ParsedChunkStore:
    return ParsedChunkStore(client=mock_client, bucket="parsed-chunks")


# ---------------------------------------------------------------------------
# _ensure_bucket
# ---------------------------------------------------------------------------


class TestEnsureBucket:
    def test_does_not_create_bucket_when_it_exists(
        self, mock_client: MagicMock
    ) -> None:
        mock_client.bucket_exists.return_value = True
        ParsedChunkStore(client=mock_client, bucket="parsed-chunks")
        mock_client.make_bucket.assert_not_called()

    def test_creates_bucket_when_it_does_not_exist(
        self, mock_client: MagicMock
    ) -> None:
        mock_client.bucket_exists.return_value = False
        ParsedChunkStore(client=mock_client, bucket="parsed-chunks")
        mock_client.make_bucket.assert_called_once_with("parsed-chunks")


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------


class TestSave:
    def test_returns_correct_key(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        key = store.save(_CHUNKS, "task-abc")
        assert key == "parsed-chunks/task-abc.json"

    def test_calls_put_object_with_correct_bucket_and_key(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        store.save(_CHUNKS, "task-abc")

        args, kwargs = mock_client.put_object.call_args
        bucket, key = args[0], args[1]
        assert bucket == "parsed-chunks"
        assert key == "parsed-chunks/task-abc.json"

    def test_serialises_chunks_correctly(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        store.save(_CHUNKS, "task-123")

        _, kwargs = mock_client.put_object.call_args
        raw_bytes = mock_client.put_object.call_args[0][2].read()
        parsed = json.loads(raw_bytes)
        assert parsed == _CHUNKS_JSON

    def test_content_type_is_json(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        store.save(_CHUNKS, "task-123")
        _, kwargs = mock_client.put_object.call_args
        assert kwargs.get("content_type") == "application/json"

    def test_length_matches_payload(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        store.save(_CHUNKS, "task-123")
        args = mock_client.put_object.call_args[0]
        data_bytes = args[2].read()  # BytesIO — already read once, need a fresh one
        # Rebuild to check length
        expected = json.dumps(_CHUNKS_JSON).encode()
        assert mock_client.put_object.call_args[1].get("length") == len(expected)

    def test_empty_chunk_list(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        key = store.save([], "empty-task")
        assert key == "parsed-chunks/empty-task.json"
        raw = mock_client.put_object.call_args[0][2].read()
        assert json.loads(raw) == []


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


class TestLoad:
    def test_deserialises_chunks_correctly(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        data = json.dumps(_CHUNKS_JSON).encode()
        mock_response = MagicMock()
        mock_response.read.return_value = data
        mock_client.get_object.return_value = mock_response

        chunks = store.load("parsed-chunks/task-abc.json")

        assert len(chunks) == 2
        assert chunks[0].page_content == "hello world"
        assert chunks[0].type == ChunkType.TEXT
        assert chunks[1].type == ChunkType.TABLE

    def test_calls_get_object_with_correct_args(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        data = json.dumps(_CHUNKS_JSON).encode()
        mock_response = MagicMock()
        mock_response.read.return_value = data
        mock_client.get_object.return_value = mock_response

        store.load("parsed-chunks/task-abc.json")

        mock_client.get_object.assert_called_once_with(
            "parsed-chunks", "parsed-chunks/task-abc.json"
        )

    def test_invalid_json_raises(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        mock_response = MagicMock()
        mock_response.read.return_value = b"not valid json"
        mock_client.get_object.return_value = mock_response

        with pytest.raises(Exception):
            store.load("parsed-chunks/bad.json")

    def test_valid_json_wrong_schema_raises(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        """A JSON list of objects missing required fields must fail Pydantic validation."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([{"wrong": "fields"}]).encode()
        mock_client.get_object.return_value = mock_response

        with pytest.raises(Exception):
            store.load("parsed-chunks/bad-schema.json")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_calls_remove_object_with_correct_args(
        self, store: ParsedChunkStore, mock_client: MagicMock
    ) -> None:
        store.delete("parsed-chunks/task-abc.json")

        mock_client.remove_object.assert_called_once_with(
            "parsed-chunks", "parsed-chunks/task-abc.json"
        )
