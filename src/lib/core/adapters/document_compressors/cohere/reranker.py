# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Cohere reranker with max_tokens_per_doc forwarded through compress_documents.

langchain-cohere's compress_documents does not forward max_tokens_per_doc to
rerank(), causing a 400 from vLLM (field must be int, not None).

Upstream fix: https://github.com/langchain-ai/langchain-cohere/pull/139
Remove this wrapper once that PR is merged and langchain-cohere is upgraded.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Optional, Sequence

from langchain_core.callbacks.manager import Callbacks
from langchain_core.documents import Document
from langchain_cohere.rerank import CohereRerank as _CohereRerank

__all__ = ["CohereRerank"]


class CohereRerank(_CohereRerank):
    """CohereRerank that always forwards max_tokens_per_doc to the rerank call.

    The value must be strictly less than the model's max_model_len.
    """

    max_tokens_per_doc: int = 511

    def compress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks: Optional[Callbacks] = None,
    ) -> Sequence[Document]:
        compressed = []
        for res in self.rerank(
            documents, query, max_tokens_per_doc=self.max_tokens_per_doc
        ):
            doc = documents[res["index"]]
            doc_copy = Document(doc.page_content, metadata=deepcopy(doc.metadata))
            doc_copy.metadata["relevance_score"] = res["relevance_score"]
            compressed.append(doc_copy)
        return compressed
