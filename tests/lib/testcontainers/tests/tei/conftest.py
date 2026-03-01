# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Shared fixtures for TEI testcontainer integration tests.

Session-scoped to avoid re-downloading model weights on every test.

Stack topology:
    host HF cache dir (tmp) → TEIContainer (BAAI/bge-reranker-base)
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from tests.lib.testcontainers.tei import TEIContainer

# Smallest available reranker model — minimises download time in CI
_RERANKER_MODEL = "BAAI/bge-reranker-base"


@pytest.fixture(scope="session")
def hf_cache_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A temporary directory mounted as the HuggingFace weight cache."""
    return tmp_path_factory.mktemp("hf-cache")


@pytest.fixture(scope="session")
def tei_reranker(hf_cache_dir: Path) -> Generator[TEIContainer, None, None]:
    """A running TEI container loaded with a reranker model.

    Model weights are cached in ``hf_cache_dir`` so they are downloaded
    only once per test session.
    """
    container = TEIContainer(_RERANKER_MODEL).with_hf_cache(str(hf_cache_dir))
    container.start()
    yield container
    container.stop()
