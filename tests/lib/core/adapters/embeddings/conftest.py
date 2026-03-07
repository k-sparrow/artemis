import requests
from typing import Tuple

import pytest
from pytest import FixtureRequest

from tests.lib.testcontainers.tei import TEIContainer


@pytest.fixture(
    params=[
        ("sentence-transformers/msmarco-MiniLM-L-12-v3", 384),
        ("nomic-ai/nomic-embed-text-v1.5", 768),
        ("BAAI/bge-m3", 1024),
    ],
    ids=lambda t: t[0],
)
def embedding_model_and_size(request: FixtureRequest) -> Tuple[str, int]:
    model_id, model_size = request.param
    tei = TEIContainer(model_id=model_id)
    tei.start()

    def stop_tei():
        tei.stop()

    request.addfinalizer(stop_tei)

    response = requests.get(tei.get_url())
    response.raise_for_status()

    return tei.get_url() + "/embed", model_size
