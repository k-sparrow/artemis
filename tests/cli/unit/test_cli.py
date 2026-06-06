"""Unit tests for the click CLI entrypoints."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from src.cli.main import cli

_SOURCE_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_NS_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_NOW = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)

_SAMPLE_DICT = {
    "id": str(_SOURCE_ID),
    "display_name": "docs-watcher",
    "source_type": "filesystem",
    "connector_name": "artemis-aaaaaaaa",
    "namespace_id": str(_NS_ID),
    "namespace_name": "docs-namespace",
    "org_name": "acme",
    "config": {"path": "/data/docs"},
    "created_at": _NOW.isoformat(),
    "updated_at": _NOW.isoformat(),
    "kafka_status": {"state": "RUNNING", "worker_id": "worker-0:8083", "tasks": []},
}


def _make_source():
    from src.backend.enterprise.data_sources.api.sources.schemas import (
        DataSourceResponse,
    )

    return DataSourceResponse.model_validate(_SAMPLE_DICT)


@pytest.fixture
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# sources list
# ---------------------------------------------------------------------------


def test_sources_list_plain(runner):
    source = _make_source()
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        MockClient.return_value.list_sources = AsyncMock(return_value=[source])
        result = runner.invoke(cli, ["sources", "list"])

    assert result.exit_code == 0
    assert "docs-watcher" in result.output
    assert "RUNNING" in result.output


def test_sources_list_json(runner):
    source = _make_source()
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        MockClient.return_value.list_sources = AsyncMock(return_value=[source])
        result = runner.invoke(cli, ["sources", "list", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data[0]["display_name"] == "docs-watcher"


# ---------------------------------------------------------------------------
# sources get
# ---------------------------------------------------------------------------


def test_sources_get_json(runner):
    source = _make_source()
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        MockClient.return_value.get_source = AsyncMock(return_value=source)
        result = runner.invoke(cli, ["sources", "get", str(_SOURCE_ID), "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["id"] == str(_SOURCE_ID)


# ---------------------------------------------------------------------------
# sources create
# ---------------------------------------------------------------------------


def test_sources_create(runner):
    source = _make_source()
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        MockClient.return_value.create_source = AsyncMock(return_value=source)
        result = runner.invoke(
            cli,
            [
                "sources",
                "create",
                "--name",
                "docs-watcher",
                "--namespace",
                "docs-namespace",
                "--org",
                "acme",
                "--path",
                "/data/docs",
            ],
        )

    assert result.exit_code == 0
    assert str(_SOURCE_ID) in result.output


# ---------------------------------------------------------------------------
# lifecycle: pause / resume / restart
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["pause", "resume", "restart"])
def test_lifecycle_commands(runner, action):
    source = _make_source()
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        getattr(
            MockClient.return_value, f"{action}_source", AsyncMock(return_value=source)
        )
        MockClient.return_value.pause_source = AsyncMock(return_value=source)
        MockClient.return_value.resume_source = AsyncMock(return_value=source)
        MockClient.return_value.restart_source = AsyncMock(return_value=source)
        result = runner.invoke(cli, ["sources", action, str(_SOURCE_ID)])

    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# sources delete
# ---------------------------------------------------------------------------


def test_sources_delete_with_yes(runner):
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        MockClient.return_value.delete_source = AsyncMock(return_value=None)
        result = runner.invoke(cli, ["sources", "delete", str(_SOURCE_ID), "--yes"])

    assert result.exit_code == 0
    assert str(_SOURCE_ID) in result.output


def test_sources_delete_prompts_without_yes(runner):
    with patch("src.cli.main.DataSourcesClient") as MockClient:
        MockClient.return_value.delete_source = AsyncMock(return_value=None)
        # Answer "n" to abort
        result = runner.invoke(cli, ["sources", "delete", str(_SOURCE_ID)], input="n\n")

    assert result.exit_code != 0
    MockClient.return_value.delete_source.assert_not_called()
