"""Pilot tests for HomeScreen."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import DataTable

from src.cli.tui.screens.home import HomeScreen

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
            await pilot.pause()  # _load worker + asyncio.gather in _update_sidebar
            assert pilot.app.screen.query_one("#home-metrics", DataTable).row_count >= 1


@pytest.mark.asyncio
async def test_sidebar_skips_empty_namespace(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    mock_storage.list_objects.return_value = []
    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert pilot.app.screen.query_one("#home-sidebar", DataTable).row_count == 0


@pytest.mark.asyncio
async def test_sidebar_shows_object(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    with (
        patch("src.cli.tui.screens.home.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.home.StorageClient", return_value=mock_storage),
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert pilot.app.screen.query_one("#home-sidebar", DataTable).row_count == 1


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
