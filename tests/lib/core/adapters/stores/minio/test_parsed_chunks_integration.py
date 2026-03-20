"""Tier 2 integration tests for ParsedChunkStore.

These tests run against a real MinIO testcontainer — no mocks.  They verify
the full save → load → delete roundtrip with actual network I/O and real
Pydantic serialisation / deserialisation.
"""

from __future__ import annotations

import pytest
from minio import Minio
from minio.error import S3Error

from src.lib.core.adapters.stores.minio.parsed_chunks import ParsedChunkStore
from src.lib.core.ingestion.types import ChunkType, ParsedChunk

_CHUNKS = [
    ParsedChunk(page_content="hello world", source="test.md", type=ChunkType.TEXT),
    ParsedChunk(page_content="| a | b |", source="report.pdf", type=ChunkType.TABLE),
]


@pytest.fixture
def store(minio_client: Minio) -> ParsedChunkStore:
    """Fresh store backed by a unique bucket per test to avoid cross-test pollution."""
    return ParsedChunkStore(client=minio_client, bucket="test-parsed-chunks")


@pytest.mark.integration
class TestParsedChunkStoreIntegration:
    def test_save_load_roundtrip(self, store: ParsedChunkStore) -> None:
        """save then load returns the identical chunks."""
        key = store.save(_CHUNKS, "roundtrip-task")
        result = store.load(key)

        assert len(result) == len(_CHUNKS)
        assert all(isinstance(c, ParsedChunk) for c in result)
        assert result[0].page_content == "hello world"
        assert result[0].source == "test.md"
        assert result[0].type == ChunkType.TEXT
        assert result[1].type == ChunkType.TABLE

    def test_save_returns_expected_key_format(self, store: ParsedChunkStore) -> None:
        key = store.save(_CHUNKS, "my-task-id")
        assert key == "parsed-chunks/my-task-id.json"

    def test_save_empty_chunks_roundtrips(self, store: ParsedChunkStore) -> None:
        key = store.save([], "empty-task")
        result = store.load(key)
        assert result == []

    def test_delete_removes_object(self, store: ParsedChunkStore) -> None:
        """After delete the object no longer exists in MinIO."""
        key = store.save(_CHUNKS, "delete-task")
        store.delete(key)

        with pytest.raises(S3Error):
            store.load(key)

    def test_bucket_created_automatically(self, minio_client: Minio) -> None:
        """ParsedChunkStore creates the bucket if it doesn't exist yet."""
        new_bucket = "auto-created-bucket"
        assert not minio_client.bucket_exists(new_bucket)

        store = ParsedChunkStore(client=minio_client, bucket=new_bucket)
        assert minio_client.bucket_exists(new_bucket)

        # cleanup
        minio_client.remove_bucket(new_bucket)

    def test_multiple_saves_with_different_task_ids(
        self, store: ParsedChunkStore
    ) -> None:
        """Each task_id produces a distinct object; they don't overwrite each other."""
        key_a = store.save(_CHUNKS[:1], "task-a")
        key_b = store.save(_CHUNKS[1:], "task-b")

        assert key_a != key_b
        result_a = store.load(key_a)
        result_b = store.load(key_b)
        assert result_a[0].page_content == "hello world"
        assert result_b[0].page_content == "| a | b |"
