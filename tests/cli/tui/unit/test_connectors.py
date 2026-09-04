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
async def test_tree_has_focus_not_hidden_filter_input(mock_ds: AsyncMock) -> None:
    """Regression test: Textual gives initial focus to the first focusable
    widget in compose() order, which is #filter-input — hidden right after
    mount, but if left focused it silently swallows every single keybinding
    on this screen (d/o/n/t/r, even '/' itself) as text typed into the
    invisible box. A real, severe bug found via a coverage sweep, not a
    hypothetical — confirmed keypresses genuinely didn't reach any action
    before on_mount started explicitly moving focus to the tree."""
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            tree = pilot.app.screen.query_one("#source-tree", Tree)
            assert tree.has_focus


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


@pytest.mark.asyncio
async def test_load_sources_error_notifies_and_keeps_tree_empty(
    mock_ds: AsyncMock,
) -> None:
    mock_ds.list_sources.side_effect = Exception("boom")
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            screen = pilot.app.screen

    assert screen._sources == []


@pytest.mark.asyncio
async def test_delete_confirmed_calls_client_and_reloads(mock_ds: AsyncMock) -> None:
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            tree = pilot.app.screen.query_one("#source-tree", Tree)
            leaf = tree.root.children[0].children[0]
            tree.move_cursor(leaf)
            await pilot.pause()

            await pilot.press("d")
            await pilot.pause()  # ConfirmScreen pushed

            await pilot.click("#yes")
            await pilot.pause()
            await pilot.pause()  # delete + reload

    mock_ds.delete_source.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_cancelled_does_not_call_client(mock_ds: AsyncMock) -> None:
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            tree = pilot.app.screen.query_one("#source-tree", Tree)
            leaf = tree.root.children[0].children[0]
            tree.move_cursor(leaf)
            await pilot.pause()

            await pilot.press("d")
            await pilot.pause()

            await pilot.click("#no")
            await pilot.pause()

    mock_ds.delete_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_failure_notifies_without_crashing(mock_ds: AsyncMock) -> None:
    mock_ds.delete_source.side_effect = Exception("boom")
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            tree = pilot.app.screen.query_one("#source-tree", Tree)
            leaf = tree.root.children[0].children[0]
            tree.move_cursor(leaf)
            await pilot.pause()

            await pilot.press("d")
            await pilot.pause()
            await pilot.click("#yes")
            await pilot.pause()
            await pilot.pause()

    mock_ds.delete_source.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_with_no_selection_warns(mock_ds: AsyncMock) -> None:
    mock_ds.list_sources.return_value = []
    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.press("d")
            await pilot.pause()

    mock_ds.delete_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_o_pushes_objects_screen_unscoped(mock_ds: AsyncMock) -> None:
    from src.cli.tui.screens.objects import ObjectsScreen

    with (
        patch("src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.objects.DataSourcesClient", return_value=mock_ds),
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            await pilot.press("o")
            await pilot.pause()
            screen = pilot.app.screen
            assert isinstance(screen, ObjectsScreen)
            assert screen._scope_ns_id is None
            assert screen._scope_group_id is None


@pytest.mark.asyncio
async def test_n_pushes_create_screen(mock_ds: AsyncMock) -> None:
    from src.cli.tui.screens.create import CreateScreen

    with patch(
        "src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            await pilot.press("n")
            await pilot.pause()
            assert isinstance(pilot.app.screen, CreateScreen)


@pytest.mark.asyncio
async def test_tree_node_selected_pushes_detail_screen(mock_ds: AsyncMock) -> None:
    from src.cli.tui.screens.detail import DetailScreen

    with (
        patch("src.cli.tui.screens.connectors.DataSourcesClient", return_value=mock_ds),
        patch("src.cli.tui.screens.detail.DataSourcesClient", return_value=mock_ds),
    ):
        async with _Host().run_test() as pilot:
            await _wait_for_sources(pilot)
            tree = pilot.app.screen.query_one("#source-tree", Tree)
            leaf = tree.root.children[0].children[0]
            tree.select_node(leaf)
            await pilot.pause()
            assert isinstance(pilot.app.screen, DetailScreen)
