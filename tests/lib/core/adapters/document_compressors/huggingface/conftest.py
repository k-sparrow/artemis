# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#

from collections.abc import Sequence
from langchain_core.documents import Document
import requests
import uuid

import pytest
from pytest import FixtureRequest

from src.lib.core.adapters.document_compressors.huggingface.reranker import (
    HuggingFaceEndpointReranker,
)
from tests.lib.testcontainers.tei import TEIContainer


@pytest.fixture(
    scope="session",
    params=[
        "BAAI/bge-reranker-base",
        "BAAI/bge-reranker-large",
        "Alibaba-NLP/gte-multilingual-reranker-base",
        "Alibaba-NLP/gte-reranker-modernbert-base",
    ],
)
def tei_model_url(request: FixtureRequest) -> str:
    tei = TEIContainer(model_id=request.param)
    tei.start()
    request.addfinalizer(lambda: tei.stop())

    response = requests.get(tei.get_url())
    response.raise_for_status()

    return tei.get_url() + "/rerank"


@pytest.fixture(
    scope="function",
    # re-reanking score threshold - filters low ranking results
    params=[0.0, 0.25, 0.5, 0.99],
)
def tei_reranker(
    tei_model_url: str,
    request: FixtureRequest,
) -> HuggingFaceEndpointReranker:
    return HuggingFaceEndpointReranker(
        model=tei_model_url,
        score_threshold=request.param,
    )


@pytest.fixture
def documents() -> Sequence[Document]:
    return [
        Document(
            page_content="Granite models allow us to do many things",
            id=uuid.uuid4().hex,
        ),
        Document(
            page_content="Documenting is the process of creating a document from information.",
            id=uuid.uuid4().hex,
        ),
        Document(
            page_content="Docling serve is a FastAPI application that is about parsing documents highly efficiently using OCR",
            id=uuid.uuid4().hex,
        ),
    ]


@pytest.fixture
def query() -> str:
    return "What is Docling?"


@pytest.fixture
def rerank_response(
    tei_reranker: HuggingFaceEndpointReranker, documents: Sequence[Document], query: str
) -> Sequence[Document]:
    response = tei_reranker.compress_documents(
        query=query,
        documents=documents,
    )
    return response


@pytest.fixture
async def async_rerank_response(
    tei_reranker: HuggingFaceEndpointReranker, documents: Sequence[Document], query: str
) -> Sequence[Document]:
    response = await tei_reranker.acompress_documents(
        query=query,
        documents=documents,
    )
    return response
