# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#

from collections.abc import Sequence

import cohere
import pytest
from pytest import FixtureRequest
from langchain_core.documents import Document

from src.lib.core.adapters.document_compressors.cohere import CohereRerank
from tests.lib.testcontainers.vllm import VLLMContainer


_COLBERT_MODEL = "colbert-ir/colbertv2.0"


@pytest.fixture(scope="session")
def vllm_rerank_url(request: FixtureRequest) -> str:
    container = VLLMContainer(model_id=_COLBERT_MODEL)
    container.start()
    request.addfinalizer(lambda: container.stop())
    return container.get_url()


@pytest.fixture(scope="session")
def reranker(vllm_rerank_url: str) -> CohereRerank:
    client = cohere.ClientV2(api_key="not-needed", base_url=vllm_rerank_url)
    return CohereRerank(model=_COLBERT_MODEL, top_n=2, client=client)


@pytest.fixture
def documents() -> Sequence[Document]:
    return [
        Document(page_content="The transformer architecture uses self-attention."),
        Document(page_content="Pancakes are made from flour, eggs, and milk."),
        Document(
            page_content="Multi-head attention allows attending to multiple subspaces."
        ),
    ]


@pytest.fixture
def query() -> str:
    return "self-attention mechanism in neural networks"


@pytest.fixture
def rerank_response(
    reranker: CohereRerank,
    documents: Sequence[Document],
    query: str,
) -> Sequence[Document]:
    return reranker.compress_documents(query=query, documents=documents)
