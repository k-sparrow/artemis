from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings

import pytest
from typing import Tuple


def test_hf_embeddings_expected_size(embedding_model_and_size: Tuple[str, int]):
    url, model_size = embedding_model_and_size
    embeddings = HuggingFaceEndpointEmbeddings(model=url)

    assert model_size == len(embeddings.embed_query("dummy"))
