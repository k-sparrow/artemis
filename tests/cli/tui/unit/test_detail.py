"""Pilot tests for DetailScreen (Epic 16.5: previously zero coverage)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import Static

from src.backend.enterprise.data_sources.api.sources.schemas import (
    ConnectorTaskStatus,
    KafkaConnectStatus,
)
from src.cli.tui.screens.detail import DetailScreen
from tests.cli.tui.unit.conftest import GRP_ID, make_source

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    async def on_mount(self) -> None:
        await self.push_screen(DetailScreen(source_id=GRP_ID, gateway_url=_GW))


@pytest.mark.asyncio
async def test_info_panel_renders_source_fields(mock_ds: AsyncMock) -> None:
    mock_ds.get_source.return_value = make_source(org_name="acme")

    with patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()  # _load worker completes
            info = str(pilot.app.screen.query_one("#info", Static).content)

    assert "acme" in info
    assert "docs-watcher" not in info  # display_name goes in the title, not #info
    assert "filesystem" in info


@pytest.mark.asyncio
async def test_tasks_panel_labeled_distinctly_from_ingestion_tasks(
    mock_ds: AsyncMock,
) -> None:
    """Epic 16.5: this panel is Kafka Connect's per-shard worker state, a
    different concept from ObjectsScreen/Home's ingestion-pipeline tasks —
    must not read as the same thing."""
    mock_ds.get_source.return_value = make_source(
        kafka_status=KafkaConnectStatus(
            state="RUNNING",
            worker_id="worker-0:8083",
            tasks=[ConnectorTaskStatus(id=0, state="RUNNING", worker_id="worker-0:8083")],
        )
    )

    with patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            tasks_text = str(pilot.app.screen.query_one("#tasks", Static).content)

    assert "Kafka Connector Tasks" in tasks_text
    assert "worker-0:8083" in tasks_text


@pytest.mark.asyncio
async def test_no_kafka_status_shows_none(mock_ds: AsyncMock) -> None:
    mock_ds.get_source.return_value = make_source(kafka_status=None)

    with patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            tasks_text = str(pilot.app.screen.query_one("#tasks", Static).content)

    assert "(none)" in tasks_text


@pytest.mark.asyncio
async def test_pause_calls_client_and_rerenders(mock_ds: AsyncMock) -> None:
    mock_ds.get_source.return_value = make_source(
        kafka_status=KafkaConnectStatus(state="RUNNING", worker_id="worker-0:8083")
    )
    mock_ds.pause_source.return_value = make_source(
        kafka_status=KafkaConnectStatus(state="PAUSED", worker_id="worker-0:8083")
    )

    with patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("p")
            await pilot.pause()
            await pilot.pause()  # _do_action worker completes

    mock_ds.pause_source.assert_awaited_once_with(GRP_ID)


@pytest.mark.asyncio
async def test_delete_confirmed_calls_client_and_pops(mock_ds: AsyncMock) -> None:
    mock_ds.get_source.return_value = make_source()
    mock_ds.delete_source.return_value = None

    with patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("d")
            await pilot.pause()  # ConfirmScreen pushed
            await pilot.click("#yes")
            await pilot.pause()
            await pilot.pause()  # delete + pop_screen

    mock_ds.delete_source.assert_awaited_once_with(GRP_ID)


@pytest.mark.asyncio
async def test_o_pushes_objects_screen_scoped_to_source(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    from src.cli.tui.screens.objects import ObjectsScreen

    mock_ds.get_source.return_value = make_source()

    with (
        patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.objects.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.objects.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("o")
            await pilot.pause()
            screen = pilot.app.screen
            assert isinstance(screen, ObjectsScreen)
            assert screen._scope_group_id == GRP_ID
