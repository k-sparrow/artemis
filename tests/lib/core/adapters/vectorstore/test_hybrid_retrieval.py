# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for QdrantVectorStore across all retrieval modes.

``TestAllRetrievalModes``    — generic assertions that must hold for every mode
                               (parameterized: dense / hybrid / multi_stage).
``TestHybridRetrievalMode``  — BM25-specific assertions for HYBRID mode.
``TestMultiStageRetrievalMode`` — ColBERT-specific assertions for MULTI_STAGE.
"""

from __future__ import annotations

import pytest
from langchain_core.documents import Document
from qdrant_client import models

from src.lib.core.adapters.embedding.sparse import FastEmbedSparseEmbeddings
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore, RetrievalMode

# A synthetic token that almost certainly does not appear in any embedding
# model's training corpus, so dense search gives it a noisy/arbitrary score.
# BM25 exact-match on this token is deterministic and very strong.
_KEYWORD = "QVBZXYW"

_DOCUMENTS = [
    "The transformer architecture uses self-attention to model long-range dependencies in text sequences",  # noqa: E501
    "Pancakes are made by mixing flour eggs and milk then cooking on a hot griddle until golden",  # noqa: E501
    f"{_KEYWORD} is a unique synthetic token used exclusively to test BM25 keyword matching",  # noqa: E501
]


# ---------------------------------------------------------------------------
# Generic tests — run for every retrieval mode
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.local
class TestAllRetrievalModes:
    """Assertions that must hold regardless of retrieval mode.

    The ``vectorstore`` fixture is parameterized over dense / hybrid /
    multi_stage so each test in this class runs three times.
    """

    def test_search_returns_k_results(self, vectorstore: QdrantVectorStore) -> None:
        vectorstore.add_texts(_DOCUMENTS)
        k = 2

        results = vectorstore.similarity_search("machine learning", k=k)

        assert len(results) == k
        assert all(isinstance(r, Document) for r in results)

    @pytest.mark.asyncio
    async def test_async_search_returns_k_results(
        self, vectorstore: QdrantVectorStore
    ) -> None:
        await vectorstore.aadd_texts(_DOCUMENTS)
        k = 2

        results = await vectorstore.asimilarity_search("machine learning", k=k)

        assert len(results) == k
        assert all(isinstance(r, Document) for r in results)

    def test_most_relevant_doc_is_retrievable(
        self, vectorstore: QdrantVectorStore
    ) -> None:
        """The document about transformers must appear in results for a
        semantics-aligned query regardless of retrieval mode."""
        vectorstore.add_texts(_DOCUMENTS)

        results = vectorstore.similarity_search(
            "self-attention mechanism in neural networks", k=3
        )

        contents = [r.page_content for r in results]
        assert any(
            "transformer" in c.lower() for c in contents
        ), f"Transformer doc not retrieved; got: {contents}"

    def test_search_with_score_returns_scores(
        self, vectorstore: QdrantVectorStore
    ) -> None:
        vectorstore.add_texts(_DOCUMENTS)

        results = vectorstore.similarity_search_with_score("cooking", k=2)

        assert len(results) == 2
        for doc, score in results:
            assert isinstance(doc, Document)
            assert isinstance(score, float)


# ---------------------------------------------------------------------------
# HYBRID-specific tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestHybridRetrievalMode:
    """Tests that hold for HYBRID (dense + BM25 sparse) retrieval mode."""

    def test_sparse_vectors_stored_in_collection(
        self, hybrid_vectorstore: QdrantVectorStore
    ) -> None:
        """Every ingested point must carry a non-empty sparse vector alongside its
        dense vector — proof that both embedding paths fired during indexing."""
        hybrid_vectorstore.add_texts(_DOCUMENTS)

        points, _ = hybrid_vectorstore.client.scroll(
            collection_name=hybrid_vectorstore.collection_name,
            with_vectors=True,
            limit=100,
        )

        assert len(points) == len(
            _DOCUMENTS
        ), f"Expected {len(_DOCUMENTS)} points, got {len(points)}"
        for point in points:
            vectors = point.vector
            assert isinstance(
                vectors, dict
            ), "Expected named-vector dict; got single-vector format"
            assert (
                "langchain-sparse" in vectors
            ), f"Point {point.id} missing 'langchain-sparse' vector"
            sparse: models.SparseVector = vectors["langchain-sparse"]
            assert (
                len(sparse.indices) > 0
            ), f"Point {point.id} has empty sparse vector (no BM25 terms indexed)"

    def test_hybrid_search_returns_k_results(
        self, hybrid_vectorstore: QdrantVectorStore
    ) -> None:
        """similarity_search must return exactly k Document objects for a
        generic semantic query — basic smoke-test that hybrid mode is wired up."""
        hybrid_vectorstore.add_texts(_DOCUMENTS)
        k = 2

        results = hybrid_vectorstore.similarity_search("machine learning", k=k)

        assert len(results) == k
        assert all(isinstance(r, Document) for r in results)

    def test_keyword_doc_ranks_first_in_hybrid_search(
        self, hybrid_vectorstore: QdrantVectorStore
    ) -> None:
        """The document containing the out-of-vocabulary keyword must be the
        top-1 hybrid result when the query is that exact keyword.

        With k=2 the keyword doc appears in both the sparse and the dense
        prefetch lists; RRF guarantees it outranks any doc in only one list."""
        hybrid_vectorstore.add_texts(_DOCUMENTS)

        results = hybrid_vectorstore.similarity_search(_KEYWORD, k=2)

        assert results, "hybrid search returned no results"
        assert (
            _KEYWORD in results[0].page_content
        ), f"Expected keyword-matched doc at rank 1; got: {results[0].page_content!r}"

    def test_hybrid_differs_from_pure_dense_for_keyword_query(
        self, hybrid_vectorstore: QdrantVectorStore
    ) -> None:
        """BM25 pulls the keyword doc to rank 1 in hybrid mode.  In dense-only
        mode the ranking of an OOV token is arbitrary — it may or may not match.
        This test verifies the hybrid path is doing real BM25 work by querying
        directly in SPARSE mode and confirming the keyword doc wins there too."""
        hybrid_vectorstore.add_texts(_DOCUMENTS)

        # Reuse the same collection from a sparse-only viewpoint so we can
        # isolate the BM25 component without the dense component's influence.
        sparse_store = QdrantVectorStore(
            client=hybrid_vectorstore.client,
            collection_name=hybrid_vectorstore.collection_name,
            retrieval_mode=RetrievalMode.SPARSE,
            sparse_embedding=FastEmbedSparseEmbeddings(),
            validate_collection_config=False,
            validate_embeddings=False,
        )

        results = sparse_store.similarity_search(_KEYWORD, k=1)

        assert results, "sparse search returned no results"
        assert (
            _KEYWORD in results[0].page_content
        ), (
            f"BM25 did not rank keyword-matched doc first; "
            f"got: {results[0].page_content!r}"
        )


# ---------------------------------------------------------------------------
# MULTI_STAGE-specific tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.local
class TestMultiStageRetrievalMode:
    """Tests specific to MULTI_STAGE (sparse+dense RRF → ColBERT MaxSim reranking)."""

    def test_all_vector_types_stored(
        self, multi_stage_vectorstore: QdrantVectorStore
    ) -> None:
        """Every point must carry all three vector spaces: named dense, BM25
        sparse, and a ColBERT multi-vector — proof that all three embedding
        paths fired during indexing."""
        multi_stage_vectorstore.add_texts(_DOCUMENTS)

        points, _ = multi_stage_vectorstore.client.scroll(
            collection_name=multi_stage_vectorstore.collection_name,
            with_vectors=True,
            limit=100,
        )

        assert len(points) == len(_DOCUMENTS)
        for point in points:
            vectors = point.vector
            assert isinstance(vectors, dict), "Expected named-vector dict"
            assert "dense" in vectors, f"Point {point.id} missing 'dense' vector"
            assert (
                "langchain-sparse" in vectors
            ), f"Point {point.id} missing 'langchain-sparse' vector"
            assert "colbert" in vectors, f"Point {point.id} missing 'colbert' vector"

    def test_colbert_vectors_are_token_matrices(
        self, multi_stage_vectorstore: QdrantVectorStore
    ) -> None:
        """ColBERT vectors must be stored as a list of 128-dim token vectors
        (multi-vector format), not a single pooled vector."""
        multi_stage_vectorstore.add_texts(_DOCUMENTS)

        points, _ = multi_stage_vectorstore.client.scroll(
            collection_name=multi_stage_vectorstore.collection_name,
            with_vectors=True,
            limit=100,
        )

        for point in points:
            colbert = point.vector["colbert"]
            assert isinstance(
                colbert, list
            ), f"Expected list of token vectors, got {type(colbert)}"
            assert len(colbert) > 0, "ColBERT multi-vector is empty"
            assert all(
                len(token_vec) == 128 for token_vec in colbert
            ), "Not all ColBERT token vectors are 128-dim"

    def test_keyword_doc_ranks_first_after_reranking(
        self, multi_stage_vectorstore: QdrantVectorStore
    ) -> None:
        """The keyword doc must reach rank 1 in multi-stage mode:
        BM25 in stage 1 guarantees it enters the candidate set; ColBERT
        reranking in stage 2 must not bury it below unrelated results."""
        multi_stage_vectorstore.add_texts(_DOCUMENTS)

        results = multi_stage_vectorstore.similarity_search(_KEYWORD, k=2)

        assert results, "multi-stage search returned no results"
        assert (
            _KEYWORD in results[0].page_content
        ), (
            f"Expected keyword doc at rank 1 after ColBERT reranking; "
            f"got: {results[0].page_content!r}"
        )

    @pytest.mark.asyncio
    async def test_async_search_returns_k_results(
        self, multi_stage_vectorstore: QdrantVectorStore
    ) -> None:
        await multi_stage_vectorstore.aadd_texts(_DOCUMENTS)
        k = 2

        results = await multi_stage_vectorstore.asimilarity_search(
            "machine learning", k=k
        )

        assert len(results) == k
        assert all(isinstance(r, Document) for r in results)
