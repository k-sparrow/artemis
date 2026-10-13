"""Unit tests for the async-path artifact assembly helpers.

No infrastructure required — all operations are pure in-memory transformations.
"""

from __future__ import annotations

import uuid


from src.backend.parsing.lib.artifact import (
    ParseStatus,
    build_pages,
    build_pages_from_markdown,
    chunk_items_to_parsed,
)
from src.lib.core.adapters.loaders.docling import PAGE_BREAK_PLACEHOLDER
from src.lib.core.ingestion.types import ChunkType, Page


_OBJ_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


class TestChunkItemsToParsed:
    def _item(
        self, text="chunk text", filename="doc.json", page_numbers=None, doc_items=None
    ):
        return {
            "text": text,
            "filename": filename,
            "page_numbers": page_numbers or [],
            **({"doc_items": doc_items} if doc_items is not None else {}),
        }

    def test_basic_mapping(self) -> None:
        item = self._item(text="hello", filename="test.pdf", page_numbers=[3])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.page_content == "hello"
        assert chunk.source == "test.pdf"
        assert chunk.page_no == 3
        assert chunk.obj_id == _OBJ_ID
        assert chunk.type == ChunkType.TEXT

    def test_table_doc_item_ref_sets_table_type(self) -> None:
        item = self._item(doc_items=["#/tables/0"])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.type == ChunkType.TABLE

    def test_table_takes_priority_over_text_in_mixed_chunk(self) -> None:
        item = self._item(doc_items=["#/texts/1", "#/tables/0", "#/texts/2"])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.type == ChunkType.TABLE

    def test_text_only_doc_items_gives_text_type(self) -> None:
        item = self._item(doc_items=["#/texts/0", "#/texts/1"])
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
        assert chunk.type == ChunkType.TEXT

    def test_missing_doc_items_defaults_to_text(self) -> None:
        item = {"text": "hello", "filename": "doc.pdf", "page_numbers": [1]}
        (chunk,) = chunk_items_to_parsed([item], _OBJ_ID)
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


class TestBuildPages:
    def _make_dl_doc(self, origin=None, with_content=False):
        """Return a DoclingDocument, with a single text item when with_content
        (non-paginated formats need real content or split_pages returns [])."""
        from docling.datamodel.document import DoclingDocument
        from docling_core.types.doc.labels import DocItemLabel

        doc = DoclingDocument(name="test", origin=origin)
        if with_content:
            doc.add_text(label=DocItemLabel.TEXT, text="hello world")
        return doc

    def _make_origin(self, filename="doc.pdf"):
        from docling_core.types.doc.document import DocumentOrigin

        return DocumentOrigin(
            mimetype="application/pdf", binary_hash=0, filename=filename
        )

    def test_returns_list_of_pages(self) -> None:
        dl_doc = self._make_dl_doc(with_content=True)
        pages = build_pages(dl_doc, _OBJ_ID)
        assert isinstance(pages, list)
        assert len(pages) == 1
        assert all(isinstance(p, Page) for p in pages)

    def test_pages_have_obj_id_stamped(self) -> None:
        dl_doc = self._make_dl_doc(with_content=True)
        pages = build_pages(dl_doc, _OBJ_ID)
        assert pages and all(p.obj_id == _OBJ_ID for p in pages)

    def test_pages_have_source_stamped_from_origin_filename(self) -> None:
        dl_doc = self._make_dl_doc(
            origin=self._make_origin(filename="report.pdf"), with_content=True
        )
        pages = build_pages(dl_doc, _OBJ_ID)
        assert pages and all(p.source == "report.pdf" for p in pages)

    def test_pages_source_falls_back_to_empty_string_when_no_origin(self) -> None:
        dl_doc = self._make_dl_doc(origin=None, with_content=True)
        pages = build_pages(dl_doc, _OBJ_ID)
        assert pages and all(p.source == "" for p in pages)


class TestBuildPagesFromMarkdown:
    """/v1/parse/resolve builds pages from the Markdown docling-serve exports
    instead of loading the DoclingDocument. Both routes must agree, or
    parent-page retrieval would change depending on which one ran."""

    def _paginated_doc(self, page_texts: list[str]):
        from docling.datamodel.document import DoclingDocument
        from docling_core.types.doc.base import BoundingBox, Size
        from docling_core.types.doc.document import DocumentOrigin, ProvenanceItem
        from docling_core.types.doc.labels import DocItemLabel

        doc = DoclingDocument(
            name="test",
            origin=DocumentOrigin(
                mimetype="application/pdf", binary_hash=0, filename="report.pdf"
            ),
        )
        for page_no, text in enumerate(page_texts, start=1):
            doc.add_page(page_no=page_no, size=Size(width=100, height=100))
            doc.add_text(
                label=DocItemLabel.TEXT,
                text=text,
                prov=ProvenanceItem(
                    page_no=page_no,
                    bbox=BoundingBox(l=0, t=0, r=10, b=10),
                    charspan=(0, len(text)),
                ),
            )
        return doc

    def _exported(self, dl_doc) -> str:
        """What docling-serve writes for to_formats=["md"] with our
        md_page_break_placeholder."""
        return dl_doc.export_to_markdown(page_break_placeholder=PAGE_BREAK_PLACEHOLDER)

    def test_matches_build_pages_for_a_paginated_document(self) -> None:
        dl_doc = self._paginated_doc(["first page", "second page", "third page"])

        pages = build_pages_from_markdown(self._exported(dl_doc), _OBJ_ID, "report.pdf")

        assert len(pages) == 3
        assert pages == build_pages(dl_doc, _OBJ_ID)

    def test_matches_build_pages_for_a_non_paginated_document(self) -> None:
        from docling.datamodel.document import DoclingDocument
        from docling_core.types.doc.labels import DocItemLabel

        dl_doc = DoclingDocument(name="test")
        dl_doc.add_text(label=DocItemLabel.TEXT, text="hello world")

        pages = build_pages_from_markdown(self._exported(dl_doc), _OBJ_ID, "")

        assert len(pages) == 1
        assert pages == build_pages(dl_doc, _OBJ_ID)

    def test_empty_markdown_yields_no_pages(self) -> None:
        assert build_pages_from_markdown("  \n", _OBJ_ID, "doc.pdf") == []


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
