"""Pilot tests for ObjectsScreen."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import DataTable, Tree

from src.cli.tui.screens.objects import ObjectsScreen
from tests.cli.tui.unit.conftest import GRP_ID, NS_ID, OWN_ID, make_task

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    def __init__(self, mock_ds: AsyncMock, mock_storage: AsyncMock) -> None:
        super().__init__()
        self._mock_ds = mock_ds
        self._mock_storage = mock_storage

    async def on_mount(self) -> None:
        with (
            patch(
                "src.cli.tui.screens.objects.DataSourcesClient",
                return_value=self._mock_ds,
            ),
            patch(
                "src.cli.tui.screens.objects.StorageClient",
                return_value=self._mock_storage,
            ),
        ):
            await self.push_screen(ObjectsScreen(gateway_url=_GW))


@pytest.mark.asyncio
async def test_groups_tree_built(mock_ds: AsyncMock, mock_storage: AsyncMock) -> None:
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()  # _load worker + _load_group complete
        tree = pilot.app.screen.query_one("#groups-tree", Tree)

    assert len(tree.root.children) == 1
    ns_node = tree.root.children[0]
    assert len(ns_node.children) == 1


@pytest.mark.asyncio
async def test_first_group_auto_loaded(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        table = pilot.app.screen.query_one("#objects-list", DataTable)
        assert table.row_count == 1


@pytest.mark.asyncio
async def test_group_load_calls_with_group_id(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()

    mock_storage.list_objects.assert_called_once_with(
        NS_ID, OWN_ID, limit=200, order="desc", group_id=GRP_ID
    )


@pytest.mark.asyncio
async def test_file_tree_hierarchy(mock_ds: AsyncMock, mock_storage: AsyncMock) -> None:
    # make_object() source is "/watch/docs/dir1/report.pdf"
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        filetree = pilot.app.screen.query_one("#objects-filetree", Tree)

    # Expected: / → watch → docs → dir1 → report.pdf (leaf)
    root_children = list(filetree.root.children)
    assert len(root_children) == 1
    watch = root_children[0]
    assert watch.label.plain == "watch"

    docs = list(watch.children)[0]
    assert docs.label.plain == "docs"

    dir1 = list(docs.children)[0]
    assert dir1.label.plain == "dir1"


@pytest.mark.asyncio
async def test_tasks_table_shows_completed_task(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """mock_storage's default make_task() is a terminal SUCCESS row — the
    Completed At cell should show a formatted timestamp, not the
    running-task stage fallback."""
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)
        row = table.get_row_at(0)

    assert str(row[1]) == "SUCCESS"
    assert "running" not in str(row[2])


@pytest.mark.asyncio
async def test_tasks_table_shows_running_task_stage(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """Epic 22: an in-flight task has no completed_at — the table must show
    its live stage there instead of crashing on None.strftime()."""
    mock_storage.list_tasks.return_value = [
        make_task(status="running", stage="tasks.submit_parse", completed_at=None)
    ]

    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)
        row = table.get_row_at(0)

    assert str(row[1]) == "running"
    assert str(row[2]) == "running (tasks.submit_parse)"


@pytest.mark.asyncio
async def test_delete_object_removes_row(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()  # objects loaded, 1 row in table

        await pilot.press("d")
        await (
            pilot.pause()
        )  # _confirm_delete_object worker starts, ConfirmScreen pushed

        await pilot.click("#yes")
        await pilot.pause()  # ConfirmScreen dismissed, delete_object called
        await pilot.pause()  # _refresh_right() rebuilds table

        table = pilot.app.screen.query_one("#objects-list", DataTable)
        assert table.row_count == 0
