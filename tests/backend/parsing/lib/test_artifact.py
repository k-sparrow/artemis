"""Unit tests for the async-path artifact assembly helpers.

No infrastructure required — all operations are pure in-memory transformations.
"""

from __future__ import annotations

import uuid


from src.backend.parsing.lib.artifact import (
    ParseStatus,
    build_artifact,
    chunk_items_to_parsed,
)
from src.lib.core.ingestion.types import ChunkType, ParseArtifact


_OBJ_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


class TestChunkItemsToParsed:
    def _item(self, text="chunk text", filename="doc.json", page_numbers=None):
        return {"text": text, "filename": filename, "page_numbers": page_numbers or []}

    def test_basic_mapping(self) -> None:
        item = self._item(text="hello", filename="test.pdf", page_numbers=[3])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.page_content == "hello"
        assert chunk.source == "test.pdf"
        assert chunk.page_no == 3
        assert chunk.obj_id == _OBJ_ID
        assert chunk.type == ChunkType.TEXT

    def test_first_page_number_used(self) -> None:
        item = self._item(page_numbers=[5, 6, 7])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.page_no == 5

    def test_missing_page_numbers_gives_none(self) -> None:
        item = self._item(page_numbers=None)
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.page_no is None

    def test_empty_page_numbers_gives_none(self) -> None:
        item = self._item(page_numbers=[])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.page_no is None

    def test_multiple_items(self) -> None:
        items = [self._item(text=f"chunk {i}", page_numbers=[i]) for i in range(5)]
        chunks = chunk_items_to_parsed(items, _OBJ_ID)
        assert len(chunks) == 5
        for i, chunk in enumerate(chunks):
            assert chunk.page_content == f"chunk {i}"
            assert chunk.page_no == i

    def test_empty_input(self) -> None:
        assert chunk_items_to_parsed([], _OBJ_ID) == []

    def test_all_chunks_have_correct_obj_id(self) -> None:
        items = [self._item() for _ in range(3)]
        chunks = chunk_items_to_parsed(items, _OBJ_ID)
        assert all(c.obj_id == _OBJ_ID for c in chunks)


class TestBuildArtifact:
    def _make_dl_doc(self):
        """Return a minimal DoclingDocument with one page of content."""
        from docling.datamodel.document import DoclingDocument

        doc = DoclingDocument(name="test")
        return doc

    def test_returns_parse_artifact(self) -> None:
        from src.backend.parsing.lib.artifact import chunk_items_to_parsed

        items = [{"text": "hello world", "filename": "doc.pdf", "page_numbers": [1]}]
        chunks = chunk_items_to_parsed(items, _OBJ_ID)
        dl_doc = self._make_dl_doc()
        artifact = build_artifact(chunks, dl_doc, _OBJ_ID)
        assert isinstance(artifact, ParseArtifact)
        assert len(artifact.chunks) == 1
        assert artifact.chunks[0].page_content == "hello world"

    def test_chunks_have_obj_id_stamped(self) -> None:
        items = [{"text": "x", "filename": "f", "page_numbers": []}]
        chunks = chunk_items_to_parsed(items, _OBJ_ID)
        dl_doc = self._make_dl_doc()
        artifact = build_artifact(chunks, dl_doc, _OBJ_ID)
        assert artifact.chunks[0].obj_id == _OBJ_ID


class TestParseStatus:
    def test_processing_status(self) -> None:
        s = ParseStatus(status="processing")
        assert s.status == "processing"
        assert s.num_processed is None

    def test_with_progress(self) -> None:
        s = ParseStatus(status="processing", num_processed=2, num_total=5)
        assert s.num_processed == 2
        assert s.num_total == 5

    def test_failure_with_message(self) -> None:
        s = ParseStatus(status="failure", error_message="OOM")
        assert s.error_message == "OOM"
