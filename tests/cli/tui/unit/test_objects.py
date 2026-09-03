"""Pilot tests for ObjectsScreen (Epic 16.5 layout: namespace picker + file
tree + tasks, replacing the old namespace/connector tree + flat object list).
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import DataTable, Select, Tree

from src.cli.tui.screens.objects import ObjectsScreen
from tests.cli.tui.unit.conftest import GRP_ID, NS_ID, OWN_ID, make_object, make_task

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    def __init__(
        self,
        mock_ds: AsyncMock,
        mock_storage: AsyncMock,
        namespace_id=None,
        group_id=None,
    ) -> None:
        super().__init__()
        self._mock_ds = mock_ds
        self._mock_storage = mock_storage
        self._namespace_id = namespace_id
        self._group_id = group_id

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
            await self.push_screen(
                ObjectsScreen(
                    gateway_url=_GW,
                    namespace_id=self._namespace_id,
                    group_id=self._group_id,
                )
            )


def _find_leaf(tree: Tree, label: str):
    stack = list(tree.root.children)
    while stack:
        node = stack.pop()
        if node.label.plain == label and not node.children:
            return node
        stack.extend(node.children)
    return None


@pytest.mark.asyncio
async def test_namespace_dropdown_populated(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    async with _Host(mock_ds, mock_storage).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        select = pilot.app.screen.query_one("#ns-select", Select)

    values = [value for _label, value in select._options]
    assert NS_ID in values


@pytest.mark.asyncio
async def test_scoped_namespace_preselected_and_loads(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """Mirrors how HomeScreen's activity row and ConnectorsScreen's tree both
    arrive here pre-scoped to a namespace."""
    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        select = pilot.app.screen.query_one("#ns-select", Select)
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)

    assert select.value == NS_ID
    assert table.row_count == 1
    mock_storage.list_objects.assert_called_once_with(
        NS_ID, OWN_ID, limit=200, order="desc", group_id=None
    )


@pytest.mark.asyncio
async def test_group_scope_from_detail_passes_group_id(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    async with _Host(
        mock_ds, mock_storage, namespace_id=NS_ID, group_id=GRP_ID
    ).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()

    mock_storage.list_objects.assert_called_once_with(
        NS_ID, OWN_ID, limit=200, order="desc", group_id=GRP_ID
    )


@pytest.mark.asyncio
async def test_file_tree_hierarchy(mock_ds: AsyncMock, mock_storage: AsyncMock) -> None:
    # make_object() source is "/watch/docs/dir1/report.pdf"
    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        filetree = pilot.app.screen.query_one("#objects-filetree", Tree)

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
    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)
        row = table.get_row_at(0)

    assert "success" in str(row[1])
    assert str(row[2]) == "tasks.index"  # make_task()'s default stage
    assert "—" != str(row[3])  # a real completed_at timestamp, not the placeholder


@pytest.mark.asyncio
async def test_tasks_table_shows_running_task_stage(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    mock_storage.list_tasks.return_value = [
        make_task(status="running", stage="tasks.submit_parse", completed_at=None)
    ]

    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)
        row = table.get_row_at(0)

    assert "running" in str(row[1])
    assert str(row[2]) == "tasks.submit_parse"


@pytest.mark.asyncio
async def test_running_task_visible_before_list_objects_has_it(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """The actual bug this screen shipped with: ingested_objects is
    CDC-populated on SUCCESS only, so a running task has no list_objects()
    row yet. Driving the tree/tasks table off list_objects() alone made an
    in-flight file invisible everywhere on this screen until it finished —
    defeating live stage visibility here specifically. Must show up via its
    task row's own `source` instead."""
    mock_storage.list_objects.return_value = []
    mock_storage.list_tasks.return_value = [
        make_task(status="running", stage="tasks.submit_parse", completed_at=None)
    ]

    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        filetree = pilot.app.screen.query_one("#objects-filetree", Tree)
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)

    assert _find_leaf(filetree, "report.pdf") is not None
    assert table.row_count == 1
    row = table.get_row_at(0)
    assert str(row[0]) == "report.pdf"
    assert "running" in str(row[1])
    assert str(row[2]) == "tasks.submit_parse"


@pytest.mark.asyncio
async def test_group_scoped_view_does_not_leak_other_connectors_running_tasks(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    """list_tasks has no group_id filter at all, so the in-flight overlay
    must NOT apply when this screen is narrowed to one connector (arriving
    from DetailScreen's 'o' key) — otherwise a running file belonging to a
    different connector in the same namespace would leak into this scoped
    view."""
    mock_storage.list_objects.return_value = []
    mock_storage.list_tasks.return_value = [
        make_task(status="running", stage="tasks.submit_parse", completed_at=None)
    ]

    async with _Host(
        mock_ds, mock_storage, namespace_id=NS_ID, group_id=GRP_ID
    ).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        filetree = pilot.app.screen.query_one("#objects-filetree", Tree)
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)

    assert _find_leaf(filetree, "report.pdf") is None
    assert table.row_count == 0


@pytest.mark.asyncio
async def test_selecting_file_filters_tasks_table(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    other = make_object(id=str(uuid.uuid4()), source="/watch/docs/dir1/other.pdf")
    mock_storage.list_objects.return_value = [make_object(), other]
    mock_storage.list_tasks.return_value = [
        make_task(source="report.pdf"),
        make_task(source="other.pdf", obj_id=str(other.id)),
    ]

    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()
        tree = pilot.app.screen.query_one("#objects-filetree", Tree)
        leaf = _find_leaf(tree, "report.pdf")
        assert leaf is not None
        tree.select_node(leaf)
        await pilot.pause()
        table = pilot.app.screen.query_one("#objects-tasks", DataTable)

    assert table.row_count == 1
    assert str(table.get_row_at(0)[0]) == "report.pdf"


@pytest.mark.asyncio
async def test_delete_object_removes_from_tree(
    mock_ds: AsyncMock, mock_storage: AsyncMock
) -> None:
    async with _Host(mock_ds, mock_storage, namespace_id=NS_ID).run_test() as pilot:
        await pilot.pause()
        await pilot.pause()  # objects loaded, one leaf in the tree

        tree = pilot.app.screen.query_one("#objects-filetree", Tree)
        leaf = _find_leaf(tree, "report.pdf")
        assert leaf is not None
        tree.select_node(leaf)
        await pilot.pause()

        await pilot.press("d")
        await pilot.pause()  # ConfirmScreen pushed

        await pilot.click("#yes")
        await pilot.pause()  # delete_object called
        await pilot.pause()  # _refresh_right() rebuilds the tree

        filetree = pilot.app.screen.query_one("#objects-filetree", Tree)
        assert _find_leaf(filetree, "report.pdf") is None

    mock_storage.delete_object.assert_awaited_once_with(NS_ID, make_object().id, OWN_ID)
