from collections import Counter
from collections.abc import Sequence

from langchain_core.documents import Document


import pytest


def test_tei_rerank_return_type(
    rerank_response: Sequence[Document],
):
    assert isinstance(rerank_response, Sequence)
    assert all(isinstance(d, Document) for d in rerank_response)


def test_tei_rerank_reranked_docs_less_than_original_docs(
    rerank_response: Sequence[Document],
    documents: Sequence[Document],
):
    assert len(rerank_response) <= len(documents)


def test_tei_rerank_reranked_docs_is_subset_of_original_docs(
    rerank_response: Sequence[Document],
    documents: Sequence[Document],
):
    doc_ids = set([d.id for d in rerank_response])
    reranked_doc_ids = set([d.id for d in documents])
    assert reranked_doc_ids.issubset(doc_ids)


def test_tei_rerank_reranked_docs_reordered(
    rerank_response: Sequence[Document],
    documents: Sequence[Document],
):
    # filter out the docs that their rank where too low
    # and extract the id of the ones who did pass the ranking score threshold
    non_filtered_doc_ids = []
    for d in documents:
        non_filtered_doc_ids.append(
            next((r.id for r in rerank_response if r.id == d.id))
        )

    # get the reranked docs ids
    reranked_doc_ids = [r.id for r in rerank_response]

    # check that the reranked docs are a permutation of the original
    # docs but not in the same order
    assert (reranked_doc_ids != non_filtered_doc_ids) and (
        Counter(reranked_doc_ids) == Counter(non_filtered_doc_ids)
    )


@pytest.mark.asyncio
async def test_tei_rerank_return_type_async(
    async_rerank_response: Sequence[Document],
):
    assert isinstance(async_rerank_response, Sequence)
    assert all(isinstance(d, Document) for d in async_rerank_response)


@pytest.mark.asyncio
async def test_tei_rerank_reranked_docs_less_than_original_docs_async(
    async_rerank_response: Sequence[Document],
    documents: Sequence[Document],
):
    assert len(async_rerank_response) <= len(documents)


@pytest.mark.asyncio
async def test_tei_rerank_reranked_docs_is_subset_of_original_docs_async(
    async_rerank_response: Sequence[Document],
    documents: Sequence[Document],
):
    doc_ids = set([d.id for d in async_rerank_response])
    reranked_doc_ids = set([d.id for d in documents])
    assert reranked_doc_ids.issubset(doc_ids)


@pytest.mark.asyncio
async def test_tei_rerank_reranked_docs_reordered_async(
    async_rerank_response: Sequence[Document],
    documents: Sequence[Document],
):
    # filter out the docs that their rank where too low
    # and extract the id of the ones who did pass the ranking score threshold
    non_filtered_doc_ids = []
    for d in documents:
        non_filtered_doc_ids.append(
            next((r.id for r in async_rerank_response if r.id == d.id))
        )

    # get the reranked docs ids
    reranked_doc_ids = [r.id for r in async_rerank_response]
    # check that the reranked docs are a permutation of the
    # original docs but not in the same order
    assert (reranked_doc_ids != non_filtered_doc_ids) and (
        Counter(reranked_doc_ids) == Counter(non_filtered_doc_ids)
    )
