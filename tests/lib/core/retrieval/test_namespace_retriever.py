# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for NamespaceRetriever against live Qdrant + TEI.

``TestNamespaceRetrieverIntegration`` — dense retrieval, no GPU required.
``TestNamespaceRetrieverWithReranker``  — full stack with ColBERT via vLLM
                                          (tags: integration, manual — GPU required).
"""

from __future__ import annotations

import uuid
from typing import List

import pytest
from langchain_core.documents import Document

from src.backend.indexing.api.retrieve.retriever import NamespaceRetriever
from src.lib.core.adapters.document_compressors.cohere import CohereRerank
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore
from src.lib.core.retrieval.adapters.simple import SimpleVectorStoreRetrieverAdapter


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _docs_with_ns(
    ns: str, texts: List[str], group_id: str | None = None, obj_id: str | None = None
) -> List[Document]:
    meta: dict = {"namespace_id": ns}
    if group_id:
        meta["group_id"] = group_id
    if obj_id:
        meta["obj_id"] = obj_id
    return [Document(page_content=t, metadata=meta) for t in texts]


def _config(ns: str, k: int = 5, **extra) -> dict:
    return {"configurable": {"namespace_id": ns, "k": k, **extra}}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def namespace_retriever(vectorstore: QdrantVectorStore) -> NamespaceRetriever:
    adapter = SimpleVectorStoreRetrieverAdapter(vectorstore)
    return NamespaceRetriever(retrieval_adapter=adapter)


@pytest.fixture
def namespace_retriever_with_reranker(
    vectorstore: QdrantVectorStore,
    reranker: CohereRerank,
) -> NamespaceRetriever:
    adapter = SimpleVectorStoreRetrieverAdapter(vectorstore)
    return NamespaceRetriever(
        retrieval_adapter=adapter,
        reranker=reranker,
        candidates_multiplier=5,
    )


# ---------------------------------------------------------------------------
# Dense retrieval — no GPU required
# ---------------------------------------------------------------------------


class TestNamespaceRetrieverIntegration:
    async def test_retrieves_documents_from_correct_namespace(
        self,
        namespace_retriever: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns_a = str(uuid.uuid4())
        ns_b = str(uuid.uuid4())

        await vectorstore.aadd_documents(
            _docs_with_ns(ns_a, ["machine learning fundamentals"])
        )
        await vectorstore.aadd_documents(
            _docs_with_ns(ns_b, ["cooking recipes for beginners"])
        )

        results = namespace_retriever.invoke("machine learning", config=_config(ns_a))

        result_ns = {d.metadata.get("namespace_id") for d in results}
        assert result_ns == {ns_a}

    async def test_namespace_isolation(
        self,
        namespace_retriever: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns_a = str(uuid.uuid4())
        ns_b = str(uuid.uuid4())

        await vectorstore.aadd_documents(
            _docs_with_ns(ns_a, ["neural networks and deep learning"])
        )

        results = namespace_retriever.invoke("neural networks", config=_config(ns_b))

        assert all(d.metadata.get("namespace_id") != ns_a for d in results)

    async def test_group_id_filter_narrows_results(
        self,
        namespace_retriever: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns = str(uuid.uuid4())
        grp_a = str(uuid.uuid4())
        grp_b = str(uuid.uuid4())

        await vectorstore.aadd_documents(
            _docs_with_ns(ns, ["transformer attention mechanisms"], group_id=grp_a)
        )
        await vectorstore.aadd_documents(
            _docs_with_ns(ns, ["convolutional neural networks"], group_id=grp_b)
        )

        results = namespace_retriever.invoke(
            "attention", config=_config(ns, group_id=grp_a)
        )

        assert all(d.metadata.get("group_id") == grp_a for d in results)

    async def test_doc_id_filter_returns_single_doc(
        self,
        namespace_retriever: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns = str(uuid.uuid4())
        obj_id = str(uuid.uuid4())
        other_id = str(uuid.uuid4())

        await vectorstore.aadd_documents(
            _docs_with_ns(ns, ["recurrent neural networks"], obj_id=obj_id)
        )
        await vectorstore.aadd_documents(
            _docs_with_ns(ns, ["graph neural networks"], obj_id=other_id)
        )

        results = namespace_retriever.invoke(
            "neural networks", config=_config(ns, doc_id=obj_id)
        )

        assert all(d.metadata.get("obj_id") == obj_id for d in results)


# ---------------------------------------------------------------------------
# Full stack with ColBERT reranker — GPU required
# ---------------------------------------------------------------------------


@pytest.mark.manual
class TestNamespaceRetrieverWithReranker:
    async def test_reranked_results_returned(
        self,
        namespace_retriever_with_reranker: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns = str(uuid.uuid4())
        await vectorstore.aadd_documents(
            _docs_with_ns(
                ns,
                [
                    "self-attention in transformer models",
                    "how to bake sourdough bread",
                    "multi-head attention for sequence modelling",
                ],
            )
        )

        results = namespace_retriever_with_reranker.invoke(
            "attention mechanism in transformers", config=_config(ns, k=2)
        )

        assert len(results) == 2
        assert all("relevance_score" in d.metadata for d in results)

    async def test_candidates_multiplier_broadens_initial_fetch(
        self,
        vectorstore: QdrantVectorStore,
        reranker: CohereRerank,
    ):
        """candidates_multiplier=5, k=2 → adapter fetches 10 docs before reranking."""
        from unittest.mock import patch

        ns = str(uuid.uuid4())
        await vectorstore.aadd_documents(
            _docs_with_ns(ns, [f"document about topic {i}" for i in range(12)])
        )

        adapter = SimpleVectorStoreRetrieverAdapter(vectorstore)
        nr = NamespaceRetriever(
            retrieval_adapter=adapter, reranker=reranker, candidates_multiplier=5
        )

        with patch.object(adapter, "get_retriever", wraps=adapter.get_retriever) as spy:
            nr.invoke("topic", config=_config(ns, k=2))
            search_kwargs = spy.call_args.kwargs["search_kwargs"]
            assert search_kwargs["k"] == 10  # 2 * 5

    async def test_reranked_scores_ordered_descending(
        self,
        namespace_retriever_with_reranker: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns = str(uuid.uuid4())
        await vectorstore.aadd_documents(
            _docs_with_ns(
                ns,
                [
                    "BERT uses bidirectional transformers for language understanding",
                    "sourdough starter requires flour and water",
                    "GPT models generate text autoregressively",
                ],
            )
        )

        results = namespace_retriever_with_reranker.invoke(
            "language model pretraining", config=_config(ns, k=2)
        )

        scores = [d.metadata["relevance_score"] for d in results]
        assert scores == sorted(scores, reverse=True)

    async def test_reranked_results_respect_namespace(
        self,
        namespace_retriever_with_reranker: NamespaceRetriever,
        vectorstore: QdrantVectorStore,
    ):
        ns_a = str(uuid.uuid4())
        ns_b = str(uuid.uuid4())

        await vectorstore.aadd_documents(
            _docs_with_ns(
                ns_a, ["attention is all you need", "transformer architecture"]
            )
        )
        await vectorstore.aadd_documents(
            _docs_with_ns(ns_b, ["unrelated content from another namespace"])
        )

        results = namespace_retriever_with_reranker.invoke(
            "transformer attention", config=_config(ns_a, k=2)
        )

        assert all(d.metadata.get("namespace_id") == ns_a for d in results)
