"""Pilot tests for ConnectorsScreen."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import DataTable, Label, Tree

from src.cli.tui.screens.connectors import ConnectorsScreen

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    async def on_mount(self) -> None:
        await self.push_screen(ConnectorsScreen(gateway_url=_GW, org_name="acme"))


@pytest.mark.asyncio
async def test_tree_built_from_sources(mock_ds: AsyncMock) -> None:
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()  # _load_sources worker completes
            tree = pilot.app.screen.query_one("#source-tree", Tree)

    assert len(tree.root.children) == 1
    ns_node = tree.root.children[0]
    assert len(ns_node.children) == 1


@pytest.mark.asyncio
async def test_filter_hides_all(mock_ds: AsyncMock) -> None:
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            # Set filter value programmatically to "zzz" — matches nothing.
            fi = pilot.app.screen.query_one("#filter-input")
            fi.display = True
            fi.value = "zzz"
            await pilot.pause()  # Input.Changed fires → _rebuild → empty-label shown
            empty_label = pilot.app.screen.query_one("#empty-label", Label)
            assert empty_label.display is True


async def _wait_for_sources(pilot) -> None:
    """Pause until _sources is populated (up to 10 ticks)."""
    screen = pilot.app.screen
    for _ in range(10):
        if screen._sources:
            return
        await pilot.pause()
    raise AssertionError(f"Sources never loaded; _sources={screen._sources}")


@pytest.mark.asyncio
async def test_toggle_shows_table(mock_ds: AsyncMock) -> None:
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            screen = pilot.app.screen
            screen.action_toggle_view()
            # Assert inside the block — Textual resets widget reactives on app exit.
            assert screen._tree_mode is False
            assert screen.query_one("#source-tree", Tree).display is False
            assert screen.query_one("#source-table", DataTable).display is True


@pytest.mark.asyncio
async def test_toggle_back_to_tree(mock_ds: AsyncMock) -> None:
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            screen = pilot.app.screen
            screen.action_toggle_view()  # tree → table
            screen.action_toggle_view()  # table → tree
            # Assert inside the block — Textual resets widget reactives on app exit.
            assert screen._tree_mode is True
            assert screen.query_one("#source-tree", Tree).display is True
            assert screen.query_one("#source-table", DataTable).display is False
