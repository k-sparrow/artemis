# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

from pathlib import Path

import pytest
from pytest import FixtureRequest

from tests.lib.testcontainers.vllm import VLLMContainer

_MODEL_ID = "colbert-ir/colbertv2.0"


@pytest.fixture(scope="session")
def vllm_container(request: FixtureRequest) -> VLLMContainer:
    hf_cache = Path.home() / ".cache" / "huggingface"
    container = VLLMContainer(model_id=_MODEL_ID)
    if hf_cache.exists():
        container.with_hf_cache(str(hf_cache))
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def vllm_base_url(vllm_container: VLLMContainer) -> str:
    return vllm_container.get_url()
