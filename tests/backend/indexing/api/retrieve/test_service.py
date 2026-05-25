# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the retrieve service singleton lifecycle."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import src.backend.indexing.api.retrieve.service as svc
from src.lib.core.retrieval.adapters.simple import SimpleVectorStoreRetrieverAdapter


@pytest.fixture(autouse=True)
def reset_singleton():
    """Ensure each test starts and ends with an uninitialised singleton."""
    svc._retriever = None
    yield
    svc._retriever = None


def _make_adapter() -> MagicMock:
    mock_vs = MagicMock()
    return SimpleVectorStoreRetrieverAdapter(mock_vs)


class TestRetrieveService:
    def test_get_retriever_raises_before_initialize(self):
        with pytest.raises(RuntimeError, match="not initialized"):
            svc.get_retriever()

    def test_get_retriever_returns_after_initialize(self):
        svc.initialize(
            retrieval_adapter=_make_adapter(),
            reranker=None,
            candidates_multiplier=10,
        )
        retriever = svc.get_retriever()
        assert retriever is not None

    def test_initialize_applies_configurable_field(self):
        svc.initialize(
            retrieval_adapter=_make_adapter(),
            reranker=None,
            candidates_multiplier=10,
        )
        retriever = svc.get_retriever()
        spec_ids = {spec.id for spec in retriever.config_specs}
        assert "namespace_id" in spec_ids

    def test_initialize_idempotent_on_second_call(self):
        adapter = _make_adapter()
        svc.initialize(adapter, None, 10)
        first = svc.get_retriever()
        svc.initialize(adapter, None, 10)
        second = svc.get_retriever()
        # second call overwrites — no error, always returns a valid retriever
        assert second is not None
        assert first is not second
