# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Pytest fixtures for ingestion library tests."""

from __future__ import annotations

from typing import List

import pytest
from langchain_core.documents import Document

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_documents() -> List[Document]:
    return [
        Document(
            page_content="This is the first test document about machine learning.",
            metadata={"source": "test1.pdf", "page": 1},
        ),
        Document(
            page_content="This is the second test document about natural language processing.",  # noqa: E501
            metadata={"source": "test1.pdf", "page": 2},
        ),
        Document(
            page_content="This is a table with data: | Name | Value | | A | 1 | | B | 2 |",  # noqa: E501
            metadata={"source": "test2.pdf", "type": "table", "page": 1},
        ),
    ]
