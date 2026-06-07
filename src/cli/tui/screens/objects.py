"""ObjectsScreen: ingested objects grouped by namespace/group with file tree and tasks."""

from __future__ import annotations

import uuid
from collections import defaultdict
from pathlib import PurePosixPath

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, Tree
from textual.widgets.tree import TreeNode

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.backend.storage.api.files.schemas import (
    IngestedObjectResponse,
    IngestionTaskResponse,
)
from src.cli.client import DataSourcesClient, StorageClient


class ObjectsScreen(Screen):
    """
    Three-panel screen:
        namespace/group tree (left) + objects list / file tree / tasks (right).
    """

    BINDINGS = [
        ("d", "delete_object", "Delete Object"),
        ("D", "delete_namespace", "Delete Namespace"),
        ("r", "refresh", "Refresh"),
        ("b", "back", "Back"),
        ("escape", "back", "Back"),
    ]

    # _groups row: (org, ns_name, ns_id, group_id, owner_id, connector_display_name)
    _GroupRow = tuple[str, str, uuid.UUID, uuid.UUID, uuid.UUID, str]

    def __init__(
        self,
        gateway_url: str | None = None,
        namespace_id: uuid.UUID | None = None,
        group_id: uuid.UUID | None = None,
    ) -> None:
        super().__init__()
        self._gateway_url = gateway_url
        self._ds_client = DataSourcesClient(base_url=gateway_url)
        self._storage_client = StorageClient(base_url=gateway_url)
        self._scope_ns_id = namespace_id
        self._scope_group_id = group_id
        self._sources: list[DataSourceResponse] = []
        self._groups: list[ObjectsScreen._GroupRow] = []
        self._objects: list[IngestedObjectResponse] = []
        self._tasks: list[IngestionTaskResponse] = []
        # Maps tree node id → group row (connector leaf) or list of group rows
        # (namespace node).
        self._leaf_map: dict[int, ObjectsScreen._GroupRow] = {}
        self._ns_map: dict[int, list[ObjectsScreen._GroupRow]] = {}
        # Tracks the namespace context active in the right panel (for namespace delete).
        self._active_ns_id: uuid.UUID | None = None
        self._active_owner_id: uuid.UUID | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Horizontal(
            Vertical(
                Label("Namespaces / Connectors", id="groups-label"),
                Tree("Sources", id="groups-tree"),
                id="objects-left",
            ),
            Vertical(
                Label("Objects", id="objects-label"),
                DataTable(id="objects-list"),
                Label("File Tree", id="tree-label"),
                Tree("Files", id="objects-filetree"),
                Label("Tasks", id="tasks-label"),
                DataTable(id="objects-tasks"),
                Label("← Select a connector or namespace.", id="objects-hint"),
                id="objects-right",
            ),
            id="objects-body",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Ingested Objects"
        self.query_one("#objects-list", DataTable).add_columns(
            "Filename", "Ingested At"
        )
        self.query_one("#objects-tasks", DataTable).add_columns(
            "Filename", "Status", "Completed At", "Reason"
        )
        self.query_one("#objects-hint").display = True
        self._load()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    @work(thread=False)
    async def _load(self) -> None:
        try:
            self._sources = await self._ds_client.list_sources()
        except Exception as exc:
            self.notify(f"Error loading sources: {exc}", severity="error")
            return

        seen: set[tuple[uuid.UUID, uuid.UUID]] = set()
        groups: list[ObjectsScreen._GroupRow] = []
        for s in self._sources:
            if self._scope_ns_id and s.namespace_id != self._scope_ns_id:
                continue
            if self._scope_group_id and s.id != self._scope_group_id:
                continue
            key = (s.namespace_id, s.id)
            if key in seen:
                continue
            seen.add(key)
            ns = s.namespace_name or str(s.namespace_id)[:8]
            groups.append(
                (s.org_name, ns, s.namespace_id, s.id, s.owner_id, s.display_name)
            )

        self._groups = groups
        first_leaf = self._rebuild_groups_tree()

        if self._scope_group_id and self._groups:
            # Opened from a connector detail — load that specific group.
            await self._load_group(self._groups[0])
        elif first_leaf is not None:
            # Auto-load the first connector's objects on mount.
            row = self._leaf_map[first_leaf.id]
            await self._load_group(row)

    def _rebuild_groups_tree(self) -> TreeNode | None:
        """
        Rebuild the namespace/connector tree.
        Returns the first connector leaf (if any).
        """
        tree = self.query_one("#groups-tree", Tree)
        tree.clear()
        self._leaf_map.clear()
        self._ns_map.clear()

        # Group rows by (org, ns_name, ns_id, owner_id).
        ns_buckets: dict[tuple, list[ObjectsScreen._GroupRow]] = defaultdict(list)
        for row in self._groups:
            org, ns, ns_id, _, owner_id, _ = row
            ns_buckets[(org, ns, ns_id, owner_id)].append(row)

        first_leaf: TreeNode | None = None
        for (org, ns, _, _), rows in ns_buckets.items():
            ns_node = tree.root.add(f"{ns}  ({org})", expand=True)
            self._ns_map[ns_node.id] = rows
            for row in rows:
                leaf = ns_node.add_leaf(row[5])  # connector display_name
                self._leaf_map[leaf.id] = row
                if first_leaf is None:
                    first_leaf = leaf

        tree.root.expand()
        return first_leaf

    # ------------------------------------------------------------------
    # Object loading — two entry points: group (filtered) or namespace (unfiltered)
    # ------------------------------------------------------------------

    async def _load_group(self, row: "ObjectsScreen._GroupRow") -> None:
        """Load objects belonging to a single connector (group_id filter)."""
        _, _, ns_id, group_id, owner_id, _ = row
        self._active_ns_id = ns_id
        self._active_owner_id = owner_id
        try:
            self._objects = await self._storage_client.list_objects(
                ns_id, owner_id, limit=200, order="desc", group_id=group_id
            )
            self._tasks = await self._storage_client.list_tasks(ns_id, owner_id)
        except Exception as exc:
            self.notify(f"Error loading objects: {exc}", severity="error")
            self._objects = []
            self._tasks = []
        self._refresh_right()

    async def _load_namespace(self, rows: list["ObjectsScreen._GroupRow"]) -> None:
        """Load all objects in a namespace across every group (no group_id filter)."""
        _, _, ns_id, _, owner_id, _ = rows[0]
        self._active_ns_id = ns_id
        self._active_owner_id = owner_id
        try:
            self._objects = await self._storage_client.list_objects(
                ns_id, owner_id, limit=200, order="desc"
            )
            self._tasks = await self._storage_client.list_tasks(ns_id, owner_id)
            self._objects.sort(key=lambda o: o.ingested_at, reverse=True)
        except Exception as exc:
            self.notify(f"Error loading objects: {exc}", severity="error")
            self._objects = []
            self._tasks = []
        self._refresh_right()

    def _refresh_right(self) -> None:
        self.query_one("#objects-hint").display = not self._objects
        self._rebuild_objects()
        self._rebuild_filetree()
        self._rebuild_tasks(selected_obj=None)

    # ------------------------------------------------------------------
    # Left-panel tree interaction
    # ------------------------------------------------------------------

    @on(Tree.NodeSelected, "#groups-tree")
    def on_groups_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        if node.id in self._leaf_map:
            self._load_group_worker(self._leaf_map[node.id])
        elif node.id in self._ns_map:
            self._load_namespace_worker(self._ns_map[node.id])

    @work(thread=False)
    async def _load_group_worker(self, row: "ObjectsScreen._GroupRow") -> None:
        await self._load_group(row)

    @work(thread=False)
    async def _load_namespace_worker(
        self, rows: list["ObjectsScreen._GroupRow"]
    ) -> None:
        await self._load_namespace(rows)

    # ------------------------------------------------------------------
    # Right-panel rebuilds
    # ------------------------------------------------------------------

    def _rebuild_objects(self) -> None:
        table = self.query_one("#objects-list", DataTable)
        table.clear()
        for obj in self._objects:
            name = obj.source.split("/")[-1]
            when = obj.ingested_at.strftime("%Y-%m-%d %H:%M")
            table.add_row(name, when)

    def _rebuild_filetree(self) -> None:
        tree = self.query_one("#objects-filetree", Tree)
        tree.clear()
        if not self._objects:
            return

        tree.root.set_label("/")
        tree.root.expand()

        dir_cache: dict[str, object] = {}

        def get_or_create_dir(path: str) -> object:
            if path in dir_cache:
                return dir_cache[path]
            p = PurePosixPath(path)
            parent_str = str(p.parent)
            if parent_str == path or parent_str in ("", ".", "/"):
                parent_node = tree.root
            else:
                parent_node = get_or_create_dir(parent_str)
            node = parent_node.add(p.name, expand=True)
            dir_cache[path] = node
            return node

        for obj in self._objects:
            p = PurePosixPath(obj.source)
            parent_str = str(p.parent)
            if parent_str in ("", ".", "/"):
                tree.root.add_leaf(p.name)
            else:
                parent_node = get_or_create_dir(parent_str)
                parent_node.add_leaf(p.name)

    def _rebuild_tasks(self, selected_obj: IngestedObjectResponse | None) -> None:
        table = self.query_one("#objects-tasks", DataTable)
        table.clear()

        task_map: dict[uuid.UUID, IngestionTaskResponse] = {}
        for t in self._tasks:
            if t.obj_id is None:
                continue
            existing = task_map.get(t.obj_id)
            if existing is None or t.completed_at > existing.completed_at:
                task_map[t.obj_id] = t

        objects_to_show = [selected_obj] if selected_obj else self._objects
        for obj in objects_to_show:
            name = obj.source.split("/")[-1]
            task = task_map.get(obj.id)
            if task:
                completed = task.completed_at.strftime("%Y-%m-%d %H:%M")
                reason = task.failure_reason or "—"
                table.add_row(name, task.status, completed, reason)
            else:
                table.add_row(name, "—", "—", "—")

    # ------------------------------------------------------------------
    # Object selection (syncs file tree and tasks)
    # ------------------------------------------------------------------

    @on(DataTable.RowSelected, "#objects-list")
    def on_object_selected(self, event: DataTable.RowSelected) -> None:
        idx = event.cursor_row
        if 0 <= idx < len(self._objects):
            obj = self._objects[idx]
            self._rebuild_tasks(selected_obj=obj)
            self._sync_tree(obj.source)

    def _sync_tree(self, source: str) -> None:
        tree = self.query_one("#objects-filetree", Tree)
        name = PurePosixPath(source).name
        stack = list(tree.root.children)
        while stack:
            node = stack.pop()
            if node.label.plain == name and not node.children:
                tree.move_cursor(node)
                return
            stack.extend(node.children)

    def action_delete_object(self) -> None:
        table = self.query_one("#objects-list", DataTable)
        idx = table.cursor_row
        if not (0 <= idx < len(self._objects)):
            self.notify("Select an object first.", severity="warning")
            return
        self._confirm_delete_object(self._objects[idx])

    def action_delete_namespace(self) -> None:
        if self._active_ns_id is None:
            self.notify("No namespace loaded.", severity="warning")
            return
        self._confirm_delete_namespace(self._active_ns_id, self._active_owner_id)

    @work(thread=False)
    async def _confirm_delete_object(self, obj: IngestedObjectResponse) -> None:
        from src.cli.tui.widgets.confirm import ConfirmScreen

        name = obj.source.split("/")[-1]
        confirmed = await self.app.push_screen_wait(ConfirmScreen(f"Delete '{name}'?"))
        if not confirmed:
            return
        try:
            await self._storage_client.delete_object(
                self._active_ns_id, obj.id, self._active_owner_id
            )
            self.notify(f"Deleted '{name}'.")
        except Exception as exc:
            self.notify(f"Delete failed: {exc}", severity="error")
            return
        # Reload current view.
        self._objects = [o for o in self._objects if o.id != obj.id]
        self._refresh_right()

    @work(thread=False)
    async def _confirm_delete_namespace(
        self, ns_id: uuid.UUID, owner_id: uuid.UUID
    ) -> None:
        from src.cli.tui.widgets.confirm import ConfirmScreen

        ns_label = next(
            (r[1] for r in self._groups if r[2] == ns_id),
            str(ns_id)[:8],
        )
        # Collect all connectors that belong to this namespace.
        ns_rows = [r for r in self._groups if r[2] == ns_id]
        connector_word = "connector" if len(ns_rows) == 1 else "connectors"
        confirmed = await self.app.push_screen_wait(
            ConfirmScreen(
                f"Delete namespace '{ns_label}' "
                f"({len(ns_rows)} {connector_word}) and ALL its objects?"
            )
        )
        if not confirmed:
            return
        try:
            await self._ds_client.delete_namespace_sources(ns_id)
            self.notify(f"Namespace '{ns_label}' and all connectors deleted.")
        except Exception as exc:
            self.notify(f"Delete failed: {exc}", severity="error")
            return
        # Prune from local state without triggering a reload.
        self._groups = [r for r in self._groups if r[2] != ns_id]
        self._active_ns_id = None
        self._active_owner_id = None
        self._objects = []
        self._tasks = []
        self._rebuild_groups_tree()
        self._refresh_right()

    def action_refresh(self) -> None:
        self._load()

    def action_back(self) -> None:
        self.app.pop_screen()
