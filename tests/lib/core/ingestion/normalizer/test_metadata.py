# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for MetadataFieldNormalizer.

Focused on the namespace UUID stamping use-case: every document that
passes through the normalizer must carry the exact same UUID value under
the ``namespace`` key, regardless of its original metadata.
"""

from __future__ import annotations

import uuid
from typing import List

import pytest
from langchain_core.documents import Document

from src.lib.core.ingestion.normalizer.metadata import MetadataFieldNormalizer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


NAMESPACE = uuid.uuid4().hex


@pytest.fixture
def namespace_normalizer() -> MetadataFieldNormalizer:
    return MetadataFieldNormalizer(fields={"namespace": NAMESPACE})


@pytest.fixture
def docs() -> List[Document]:
    return [
        Document(page_content="first doc", metadata={"source": "a.pdf"}),
        Document(page_content="second doc", metadata={"source": "b.pdf"}),
        Document(page_content="third doc", metadata={"source": "c.pdf", "page": 3}),
    ]


# ---------------------------------------------------------------------------
# Namespace field is stamped on every document
# ---------------------------------------------------------------------------


class TestNamespaceStamping:
    def test_all_outputs_carry_namespace(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        result = namespace_normalizer.normalize(docs)
        assert all(doc.metadata.get("namespace") == NAMESPACE for doc in result)

    def test_all_outputs_carry_same_namespace(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        result = namespace_normalizer.normalize(docs)
        namespaces = [doc.metadata["namespace"] for doc in result]
        assert len(set(namespaces)) == 1

    def test_namespace_value_matches_configured_uuid(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        result = namespace_normalizer.normalize(docs)
        assert all(doc.metadata["namespace"] == NAMESPACE for doc in result)

    def test_output_count_matches_input_count(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        result = namespace_normalizer.normalize(docs)
        assert len(result) == len(docs)

    def test_existing_metadata_fields_are_preserved(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        result = namespace_normalizer.normalize(docs)
        assert result[0].metadata["source"] == "a.pdf"
        assert result[2].metadata["source"] == "c.pdf"
        assert result[2].metadata["page"] == 3

    def test_page_content_is_unchanged(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        result = namespace_normalizer.normalize(docs)
        for original, normalized in zip(docs, result):
            assert normalized.page_content == original.page_content

    def test_original_documents_are_not_mutated(
        self, namespace_normalizer: MetadataFieldNormalizer, docs: List[Document]
    ) -> None:
        namespace_normalizer.normalize(docs)
        assert all("namespace" not in doc.metadata for doc in docs)

    def test_existing_namespace_is_overwritten(self) -> None:
        doc = Document(page_content="x", metadata={"namespace": "old-value"})
        normalizer = MetadataFieldNormalizer(fields={"namespace": NAMESPACE})
        result = normalizer.normalize([doc])
        assert result[0].metadata["namespace"] == NAMESPACE

    def test_empty_input_returns_empty_list(
        self, namespace_normalizer: MetadataFieldNormalizer
    ) -> None:
        assert namespace_normalizer.normalize([]) == []

    def test_single_document(self) -> None:
        ns = uuid.uuid4().hex
        normalizer = MetadataFieldNormalizer(fields={"namespace": ns})
        doc = Document(page_content="solo", metadata={})
        result = normalizer.normalize([doc])
        assert len(result) == 1
        assert result[0].metadata["namespace"] == ns


# ---------------------------------------------------------------------------
# async variant produces identical results
# ---------------------------------------------------------------------------


class TestAsyncNormalize:
    @pytest.mark.asyncio
    async def test_anormalize_all_outputs_carry_namespace(
        self,
        namespace_normalizer: MetadataFieldNormalizer,
        docs: List[Document],
    ) -> None:
        result = await namespace_normalizer.anormalize(docs)
        assert all(doc.metadata.get("namespace") == NAMESPACE for doc in result)

    @pytest.mark.asyncio
    async def test_anormalize_matches_sync(
        self,
        namespace_normalizer: MetadataFieldNormalizer,
        docs: List[Document],
    ) -> None:
        sync_result = namespace_normalizer.normalize(docs)
        async_result = await namespace_normalizer.anormalize(docs)
        assert len(sync_result) == len(async_result)
        for s, a in zip(sync_result, async_result):
            assert s.page_content == a.page_content
            assert s.metadata == a.metadata
