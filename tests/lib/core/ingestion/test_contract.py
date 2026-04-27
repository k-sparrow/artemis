"""Unit tests for src/lib/core/ingestion/contract.py."""

from __future__ import annotations

import json
import uuid

import pytest  # noqa: F401

from src.lib.core.ingestion.contract import IngestionInfo


class TestIngestionInfo:
    def test_roundtrip_without_group_id(self) -> None:
        namespace_id = uuid.uuid4()
        info = IngestionInfo(namespace_id=namespace_id)
        restored = IngestionInfo.model_validate_json(info.model_dump_json())
        assert restored.namespace_id == namespace_id
        assert restored.group_id is None

    def test_roundtrip_with_group_id(self) -> None:
        namespace_id = uuid.uuid4()
        group_id = uuid.uuid4()
        info = IngestionInfo(namespace_id=namespace_id, group_id=group_id)
        restored = IngestionInfo.model_validate_json(info.model_dump_json())
        assert restored.namespace_id == namespace_id
        assert restored.group_id == group_id

    def test_group_id_defaults_to_none(self) -> None:
        info = IngestionInfo(namespace_id=uuid.uuid4())
        assert info.group_id is None

    def test_serialised_json_omits_null_group_id(self) -> None:
        info = IngestionInfo(namespace_id=uuid.uuid4())
        data = json.loads(info.model_dump_json())
        assert "group_id" in data
        assert data["group_id"] is None

    def test_group_id_present_in_serialised_json(self) -> None:
        group_id = uuid.uuid4()
        info = IngestionInfo(namespace_id=uuid.uuid4(), group_id=group_id)
        data = json.loads(info.model_dump_json())
        assert data["group_id"] == str(group_id)
