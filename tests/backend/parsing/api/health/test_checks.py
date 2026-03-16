# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for parsing service health checks.

DoclingServeHealthcheck is tested against a real Docling testcontainer so the
HTTP interaction is genuine, not mocked.
"""

from __future__ import annotations

import pytest

from src.backend.parsing.api.health.checks import DoclingServeHealthcheck, LivenessCheck


class TestLivenessCheck:
    @pytest.mark.asyncio
    async def test_liveness_always_passes(self) -> None:
        check = LivenessCheck()
        result = await check()
        assert result.passed is True
        assert result.name == "Liveness"


class TestDoclingServeHealthcheck:
    @pytest.mark.asyncio
    async def test_passes_when_docling_is_healthy(
        self, docling_health_url: str
    ) -> None:
        check = DoclingServeHealthcheck(url=docling_health_url)
        result = await check()
        assert result.passed is True
        assert result.name == "Readiness/Docling-Serve"

    @pytest.mark.asyncio
    async def test_fails_when_docling_unreachable(self) -> None:
        # Port 1 is reserved / never open — guaranteed connection refused.
        check = DoclingServeHealthcheck(url="http://127.0.0.1:1/health", timeout=2)
        result = await check()
        assert result.passed is False
        assert result.name == "Readiness/Docling-Serve"
