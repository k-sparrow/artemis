# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for pre_req_config_modifier."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.backend.indexing.api.retrieve.utils import pre_req_config_modifier


def _make_request(configurable: dict) -> AsyncMock:
    request = AsyncMock()
    request.json = AsyncMock(return_value={"config": {"configurable": configurable}})
    return request


class TestPreReqConfigModifier:
    @pytest.mark.asyncio
    async def test_raises_400_when_namespace_id_missing(self):
        request = _make_request({})
        with pytest.raises(HTTPException) as exc_info:
            await pre_req_config_modifier({}, request)
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_passes_through_when_namespace_id_present(self):
        request = _make_request({"namespace_id": "ns-abc"})
        result = await pre_req_config_modifier({}, request)
        assert result["configurable"]["namespace_id"] == "ns-abc"

    @pytest.mark.asyncio
    async def test_merges_configurable_from_request_body(self):
        request = _make_request({"namespace_id": "ns-x", "group_id": "grp-1"})
        result = await pre_req_config_modifier({}, request)
        assert result["configurable"]["namespace_id"] == "ns-x"
        assert result["configurable"]["group_id"] == "grp-1"

    @pytest.mark.asyncio
    async def test_creates_configurable_key_if_missing(self):
        request = _make_request({"namespace_id": "ns-y"})
        # incoming config has no "configurable" key at all
        result = await pre_req_config_modifier({}, request)
        assert "configurable" in result
        assert result["configurable"]["namespace_id"] == "ns-y"

    @pytest.mark.asyncio
    async def test_existing_configurable_entries_preserved(self):
        request = _make_request({"namespace_id": "ns-z"})
        existing = {"configurable": {"pre_existing_key": "value"}}
        result = await pre_req_config_modifier(existing, request)
        assert result["configurable"]["pre_existing_key"] == "value"
        assert result["configurable"]["namespace_id"] == "ns-z"
