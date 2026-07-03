"""Unit tests for PDF shard splitting.

No infrastructure required — pypdf operates purely in memory.
"""

from __future__ import annotations

import math
from io import BytesIO

from pypdf import PdfReader, PdfWriter

from src.backend.parsing.lib.sharding import pdf_page_count, split_pdf_shards


def _make_pdf(num_pages: int) -> bytes:
    writer = PdfWriter()
    for _ in range(num_pages):
        writer.add_blank_page(width=100, height=100)
    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()


class TestPdfPageCount:
    def test_returns_correct_count(self) -> None:
        assert pdf_page_count(_make_pdf(7)) == 7

    def test_single_page(self) -> None:
        assert pdf_page_count(_make_pdf(1)) == 1


class TestSplitPdfShards:
    def test_exact_multiple_produces_correct_shard_count(self) -> None:
        pdf = _make_pdf(400)
        shards = split_pdf_shards(pdf, page_limit=200)
        assert len(shards) == 2

    def test_uneven_split_rounds_up(self) -> None:
        pdf = _make_pdf(401)
        shards = split_pdf_shards(pdf, page_limit=400)
        assert len(shards) == 2

    def test_single_shard_when_below_limit(self) -> None:
        pdf = _make_pdf(10)
        shards = split_pdf_shards(pdf, page_limit=400)
        assert len(shards) == 1

    def test_total_pages_preserved(self) -> None:
        n = 850
        pdf = _make_pdf(n)
        shards = split_pdf_shards(pdf, page_limit=400)
        total = sum(len(PdfReader(BytesIO(s)).pages) for s in shards)
        assert total == n

    def test_each_shard_at_most_page_limit(self) -> None:
        limit = 300
        pdf = _make_pdf(700)
        shards = split_pdf_shards(pdf, page_limit=limit)
        for shard in shards:
            assert len(PdfReader(BytesIO(shard)).pages) <= limit

    def test_shard_count_matches_ceil(self) -> None:
        for n in (1, 399, 400, 401, 800, 1201):
            limit = 400
            shards = split_pdf_shards(_make_pdf(n), page_limit=limit)
            assert len(shards) == math.ceil(n / limit)

    def test_returns_valid_pdf_bytes(self) -> None:
        pdf = _make_pdf(5)
        shards = split_pdf_shards(pdf, page_limit=3)
        for shard in shards:
            # pypdf should parse without error
            reader = PdfReader(BytesIO(shard))
            assert len(reader.pages) >= 1
