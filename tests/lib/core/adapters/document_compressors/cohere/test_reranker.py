from collections.abc import Sequence

from langchain_core.documents import Document


class TestCohereRerank:
    def test_returns_documents(self, rerank_response: Sequence[Document]) -> None:
        assert len(rerank_response) > 0
        assert all(isinstance(d, Document) for d in rerank_response)

    def test_respects_top_n(self, rerank_response: Sequence[Document]) -> None:
        # reranker is configured with top_n=2; input has 3 docs
        assert len(rerank_response) <= 2

    def test_result_is_subset_of_input(
        self,
        rerank_response: Sequence[Document],
        documents: Sequence[Document],
    ) -> None:
        input_contents = {d.page_content for d in documents}
        for doc in rerank_response:
            assert doc.page_content in input_contents

    def test_adds_relevance_score(self, rerank_response: Sequence[Document]) -> None:
        for doc in rerank_response:
            assert "relevance_score" in doc.metadata
            assert isinstance(doc.metadata["relevance_score"], float)

    def test_scores_ordered_descending(
        self, rerank_response: Sequence[Document]
    ) -> None:
        scores = [d.metadata["relevance_score"] for d in rerank_response]
        assert scores == sorted(scores, reverse=True)
