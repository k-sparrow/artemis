# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

import pytest
from pytest import FixtureRequest

from tests.lib.testcontainers.vllm import VLLMContainer

_MODELS = [
    ("colbert-ir/colbertv2.0", False, None),
    ("jinaai/jina-colbert-v2", True, {"architectures": ["ColBERTJinaRobertaModel"]}),
]


@pytest.fixture(
    scope="session",
    params=_MODELS,
    ids=[m[0] for m in _MODELS],
)
def vllm_container(request: FixtureRequest) -> VLLMContainer:
    model_id, trust_remote_code, hf_overrides = request.param
    container = VLLMContainer(model_id=model_id)
    if hf_overrides:
        container.with_hf_overrides(hf_overrides)
    if trust_remote_code:
        container.with_trust_remote_code()
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def vllm_base_url(vllm_container: VLLMContainer) -> str:
    return vllm_container.get_url()


@pytest.fixture(scope="session")
def vllm_model_id(vllm_container: VLLMContainer) -> str:
    return vllm_container.model_id
