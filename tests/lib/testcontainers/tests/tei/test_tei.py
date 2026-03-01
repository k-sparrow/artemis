# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Tests for TEIContainer.

Unit tests run without Docker and validate configuration state.
Integration tests start a real container and verify runtime behaviour.

Run unit tests only:
    pytest tests/lib/testcontainers/tests/tei/test_tei.py -m "not integration" -v

Run integration tests (requires Docker + network access):
    pytest tests/lib/testcontainers/tests/tei/test_tei.py -m integration -v
"""

from __future__ import annotations

from pathlib import Path

import pytest
import requests
from testcontainers.core.wait_strategies import HttpWaitStrategy

from src.lib.core.adapters.document_compressors.huggingface._client import (
    AsyncInferenceClient,
    InferenceClient,
)
from tests.lib.testcontainers.tei import TEIContainer

_MODEL = "BAAI/bge-reranker-base"


# ---------------------------------------------------------------------------
# Unit tests — no Docker required
# ---------------------------------------------------------------------------


class TestTEIContainerDefaults:
    """Verify default configuration after construction."""

    def test_default_image(self):
        c = TEIContainer(_MODEL)
        assert c.image == TEIContainer.DEFAULT_IMAGE

    def test_http_port_exposed(self):
        c = TEIContainer(_MODEL)
        assert TEIContainer.HTTP_PORT in c.ports

    def test_model_id_in_command(self):
        c = TEIContainer(_MODEL)
        assert f'--model-id "{_MODEL}"' in c._command

    def test_model_id_property(self):
        c = TEIContainer(_MODEL)
        assert c.model_id == _MODEL

    def test_wait_strategy_is_http(self):
        c = TEIContainer(_MODEL)
        assert isinstance(c._wait_strategy, HttpWaitStrategy)

    def test_custom_image(self):
        custom = "ghcr.io/huggingface/text-embeddings-inference:cpu-1.5"
        c = TEIContainer(_MODEL, image=custom)
        assert c.image == custom

    def test_different_model_ids_produce_different_commands(self):
        c1 = TEIContainer("BAAI/bge-reranker-base")
        c2 = TEIContainer("sentence-transformers/msmarco-MiniLM-L-12-v3")
        assert c1._command != c2._command


class TestTEIContainerFluentMethods:
    """Verify fluent configuration methods."""

    def test_with_hf_cache_returns_self(self, tmp_path: Path):
        c = TEIContainer(_MODEL)
        result = c.with_hf_cache(str(tmp_path))
        assert result is c

    def test_with_hf_cache_registers_volume_binding(self, tmp_path: Path):
        c = TEIContainer(_MODEL)
        c.with_hf_cache(str(tmp_path))
        assert str(tmp_path) in c.volumes
        assert c.volumes[str(tmp_path)]["bind"] == "/data"

    def test_with_hf_cache_mounts_read_write(self, tmp_path: Path):
        c = TEIContainer(_MODEL)
        c.with_hf_cache(str(tmp_path))
        assert c.volumes[str(tmp_path)]["mode"] == "rw"

    def test_with_hf_cache_chaining(self, tmp_path: Path):
        """with_hf_cache can be chained with other fluent methods."""
        c = TEIContainer(_MODEL).with_hf_cache(str(tmp_path))
        assert c.model_id == _MODEL


# ---------------------------------------------------------------------------
# Integration tests — require Docker and network access
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTEIContainerIntegration:
    """Verify TEIContainer behaviour against a live container."""

    def test_info_endpoint_returns_200(self, tei_reranker: TEIContainer):
        """GET /info must return HTTP 200 once the model is loaded."""
        response = requests.get(tei_reranker.get_url() + "/info", timeout=10)
        assert response.status_code == 200

    def test_info_reports_expected_model_id(self, tei_reranker: TEIContainer):
        """GET /info must reference the model that was passed at construction."""
        info = requests.get(tei_reranker.get_url() + "/info", timeout=10).json()
        assert info.get("model_id") == tei_reranker.model_id

    def test_client_returns_inference_client_instance(self, tei_reranker: TEIContainer):
        """client() must return the patched InferenceClient."""
        assert isinstance(tei_reranker.client(), InferenceClient)

    def test_async_client_returns_async_inference_client_instance(
        self, tei_reranker: TEIContainer
    ):
        """async_client() must return the patched AsyncInferenceClient."""
        assert isinstance(tei_reranker.async_client(), AsyncInferenceClient)
