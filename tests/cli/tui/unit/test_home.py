"""Pilot tests for HomeScreen."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import DataTable

from src.cli.tui.screens.home import HomeScreen
from tests.cli.tui.unit.conftest import make_task

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    async def on_mount(self) -> None:
        await self.push_screen(HomeScreen(gateway_url=_GW, org_name="acme"))


@pytest.mark.asyncio
async def test_metrics_populated(mock_ds: AsyncMock, mock_storage: AsyncMock) -> None:
    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()  # _load worker + asyncio.gather in _update_activity
            assert pilot.app.screen.query_one("#home-metrics", DataTable).row_count >= 1


@pytest.mark.asyncio
async def test_activity_skips_namespace_with_no_tasks(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    mock_storage.list_tasks.return_value = []
    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert (
                pilot.app.screen.query_one("#home-activity", DataTable).row_count == 0
            )


@pytest.mark.asyncio
async def test_activity_shows_task(mock_ds: AsyncMock, mock_storage: AsyncMock) -> None:
    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert (
                pilot.app.screen.query_one("#home-activity", DataTable).row_count == 1
            )


@pytest.mark.asyncio
async def test_activity_shows_running_task_stage(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """Epic 16.5's whole point: a running task's live stage must be visible
    on Home, not just terminal outcomes."""
    mock_storage.list_tasks.return_value = [
        make_task(status="running", stage="tasks.submit_parse", completed_at=None)
    ]
    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            table = pilot.app.screen.query_one("#home-activity", DataTable)
            row = table.get_row_at(0)

    assert "running" in str(row[2])
    assert str(row[3]) == "tasks.submit_parse"


@pytest.mark.asyncio
async def test_activity_shows_only_a_small_recent_window(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """Home is a glance, not a full activity log — even if a namespace has
    many tasks, only the _ACTIVITY_DISPLAY_LIMIT most recent overall show,
    most-recent first. Full history belongs on ObjectsScreen."""
    import datetime

    base = datetime.datetime(2026, 6, 5, 12, 0, tzinfo=datetime.timezone.utc)
    tasks = [
        make_task(
            source=f"doc-{i}.pdf",
            created_at=(base + datetime.timedelta(minutes=i)).isoformat(),
            completed_at=(base + datetime.timedelta(minutes=i)).isoformat(),
        )
        for i in range(10)
    ]
    mock_storage.list_tasks.return_value = tasks

    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            table = pilot.app.screen.query_one("#home-activity", DataTable)
            row_count = table.row_count
            first_row_object = str(table.get_row_at(0)[1])

    assert row_count == 8
    assert first_row_object == "doc-9.pdf"  # the most recent of the 10


@pytest.mark.asyncio
async def test_activity_row_selected_navigates_to_objects_screen(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    from src.cli.tui.screens.objects import ObjectsScreen

    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
        patch("src.cli.tui.screens.objects.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.objects.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            table = pilot.app.screen.query_one("#home-activity", DataTable)
            table.focus()
            table.move_cursor(row=0)
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            assert isinstance(pilot.app.screen, ObjectsScreen)


@pytest.mark.asyncio
async def test_s_pushes_connectors_screen(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    from src.cli.tui.screens.connectors import ConnectorsScreen

    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
        patch("src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("s")
            await pilot.pause()
            assert isinstance(pilot.app.screen, ConnectorsScreen)


@pytest.mark.asyncio
async def test_n_pushes_create_screen(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    from src.cli.tui.screens.create import CreateScreen

    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
        patch("src.cli.tui.screens.create.DataSourcesClient", return_value=mock_ds),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("n")
            await pilot.pause()
            assert isinstance(pilot.app.screen, CreateScreen)
