"""ObjectsScreen: namespace-scoped file tree + task status for ingested objects."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import PurePosixPath

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, Select, Tree
from textual.widgets.tree import TreeNode

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.backend.storage.api.files.schemas import IngestionTaskResponse
from src.cli.client import DataSourcesClient, StorageClient
from src.cli.tui.widgets.status_badge import task_status_markup

# Namespace-wide task fetch cap (Epic 16.5) — ingestion_status keeps one row
# per object ever ingested, so this stays bounded like Home's activity poll.
_TASKS_LIMIT = 200


@dataclass(frozen=True)
class _FileEntry:
    """A file this screen knows about — either a completed IngestedObject or
    a still-in-flight one we only know about via its task row.

    ingested_objects is populated ONLY on SUCCESS, via CDC (see
    src/backend/storage/api/models.py) — a running task has no row there
    yet. Driving the file tree / tasks table off list_objects() alone means
    an in-flight file is invisible everywhere on this screen until it
    finishes and CDC catches up, silently defeating the entire point of
    Epic 22's live stage visibility here specifically. Only `.id`/`.source`
    are ever used in this file, so this is deliberately not the full
    IngestedObjectResponse — it has to represent objects that don't have
    one yet.
    """

    id: uuid.UUID
    source: str


class ObjectsScreen(Screen):
    """
    Namespace-picker on top; file tree (primary) + task status side by side
    below. Replaces the old namespace/connector tree + flat objects table —
    the file tree already browses the same objects hierarchically, and
    connector-level drill-down is still reachable via DetailScreen's 'o' key
    (which pre-scopes this screen to one connector via group_id).
    """

    BINDINGS = [
        ("d", "delete_object", "Delete Object"),
        ("D", "delete_namespace", "Delete Namespace"),
        ("r", "refresh", "Refresh"),
        ("b", "back", "Back"),
        ("escape", "back", "Back"),
    ]

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
        # Namespace dropdown backing state — one representative owner_id per
        # namespace (same "any source in this namespace will do" pattern
        # HomeScreen already uses for its own per-namespace fan-out).
        self._ns_owner: dict[uuid.UUID, uuid.UUID] = {}
        self._files: list[_FileEntry] = []
        self._tasks: list[IngestionTaskResponse] = []
        # obj_id-independent leaf lookup — file tree leaf node id → file,
        # so selecting a leaf can filter the tasks table and deletion can
        # target it, without a separate flat objects table driving either.
        self._file_leaf_map: dict[int, _FileEntry] = {}
        # Tracks the namespace/group context currently shown — for delete
        # actions and for _refresh_active's periodic in-place reload (must
        # NOT re-trigger the "user changed the dropdown" group-drop logic).
        self._active_ns_id: uuid.UUID | None = None
        self._active_owner_id: uuid.UUID | None = None
        self._active_group_id: uuid.UUID | None = None
        self._selected_obj: _FileEntry | None = None
        # Guards the one programmatic Select.value set during initial load
        # from being treated as a user-driven namespace change (which drops
        # any group_id scope — see on_namespace_selected).
        self._suspend_ns_select_handler = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Horizontal(
                Label("Namespace:", id="ns-select-label"),
                Select[uuid.UUID](
                    [], id="ns-select", prompt="Choose a namespace...", allow_blank=True
                ),
                id="objects-toolbar",
            ),
            Horizontal(
                Vertical(
                    Label("Files", id="tree-label"),
                    Tree("/", id="objects-filetree"),
                    id="objects-tree-panel",
                ),
                Vertical(
                    Label("Tasks", id="tasks-label"),
                    DataTable(id="objects-tasks", cursor_type="row"),
                    id="objects-tasks-panel",
                ),
                id="objects-body",
            ),
            Label("← Choose a namespace.", id="objects-hint"),
            id="objects-content",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Ingested Objects"
        self.query_one("#objects-tasks", DataTable).add_columns(
            "Filename", "Status", "Stage", "Completed At", "Reason"
        )
        self.query_one("#objects-hint").display = True
        self._load()
        self.set_interval(10, self._refresh_active)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    @work(thread=False)
    async def _load(self) -> None:
        # Full reload (mount or manual 'r') — repopulates the namespace
        # dropdown; any object-row filter from before is no longer
        # necessarily valid.
        self._selected_obj = None
        try:
            self._sources = await self._ds_client.list_sources()
        except Exception as exc:
            self.notify(f"Error loading sources: {exc}", severity="error")
            return

        self._rebuild_ns_select()

        if self._scope_ns_id is not None and self._scope_ns_id in self._ns_owner:
            owner_id = self._ns_owner[self._scope_ns_id]
            select = self.query_one("#ns-select", Select)
            # Select.Changed is posted, not fired synchronously — it won't
            # actually be handled until a later message-pump tick (after
            # this coroutine's next await), so the guard must be consumed
            # by the handler itself, not reset right after this line.
            self._suspend_ns_select_handler = True
            select.value = self._scope_ns_id
            await self._load_selection(
                self._scope_ns_id, owner_id, self._scope_group_id
            )

    def _rebuild_ns_select(self) -> None:
        # One representative source per namespace — same enumeration this
        # screen has always used (there's no standalone "list namespaces"
        # client call today; a namespace with zero connectors is invisible
        # here, same limitation the old namespace/connector tree also had).
        ns_map: dict[uuid.UUID, DataSourceResponse] = {}
        for s in self._sources:
            if s.namespace_id not in ns_map:
                ns_map[s.namespace_id] = s
        self._ns_owner = {ns_id: s.owner_id for ns_id, s in ns_map.items()}

        options = sorted(
            (
                (f"{s.namespace_name or str(ns_id)[:8]}  ({s.org_name})", ns_id)
                for ns_id, s in ns_map.items()
            ),
            key=lambda opt: opt[0],
        )
        self.query_one("#ns-select", Select).set_options(options)

    async def _load_selection(
        self,
        ns_id: uuid.UUID,
        owner_id: uuid.UUID,
        group_id: uuid.UUID | None,
    ) -> None:
        """Load every object (and task) in a namespace, optionally narrowed
        to one connector (group_id) — the only remaining case for that
        narrowing is arriving pre-scoped from DetailScreen's 'o' key."""
        self._active_ns_id = ns_id
        self._active_owner_id = owner_id
        self._active_group_id = group_id
        try:
            objects = await self._storage_client.list_objects(
                ns_id, owner_id, limit=200, order="desc", group_id=group_id
            )
            self._tasks = await self._storage_client.list_tasks(
                ns_id, owner_id, limit=_TASKS_LIMIT, order="desc"
            )
        except Exception as exc:
            self.notify(f"Error loading objects: {exc}", severity="error")
            objects = []
            self._tasks = []
        self._files = self._merge_files(objects, group_id)
        self._refresh_right()

    def _merge_files(self, objects, group_id: uuid.UUID | None) -> list[_FileEntry]:
        """list_objects() only has SUCCESS objects (CDC-fed); a running task
        has no row there yet. Overlay any in-flight task not already covered
        so the file tree / tasks table can show it too — the entire point of
        putting live stage on this screen in the first place.

        list_tasks() has no group_id filter (IngestionTaskResponse doesn't
        even carry group_id) — safe to overlay unfiltered when browsing the
        whole namespace, but NOT when narrowed to one connector (group_id
        set), where it would leak in-flight files from other connectors in
        the same namespace. That narrower path — reached only via
        DetailScreen's 'o' key — keeps the pre-existing objects-only
        behavior until list_tasks can be scoped by group_id too.
        """
        files = [_FileEntry(id=o.id, source=o.source) for o in objects]
        if group_id is not None:
            return files
        known_ids = {f.id for f in files}
        for t in self._tasks:
            if t.obj_id is None or t.obj_id in known_ids or t.source is None:
                continue
            files.append(_FileEntry(id=t.obj_id, source=t.source))
            known_ids.add(t.obj_id)
        return files

    def _refresh_right(self) -> None:
        self.query_one("#objects-hint").display = not self._files
        self._rebuild_filetree()
        self._rebuild_tasks(selected_obj=self._selected_obj)

    @work(thread=False)
    async def _refresh_active(self) -> None:
        """Periodic refresh (Epic 16.5): re-fetch whichever namespace/group
        is currently shown, in place. Deliberately does NOT call _load() —
        that would silently reset the dropdown/selection every 10s."""
        if self._active_ns_id is not None and self._active_owner_id is not None:
            await self._load_selection(
                self._active_ns_id, self._active_owner_id, self._active_group_id
            )

    # ------------------------------------------------------------------
    # Namespace picker
    # ------------------------------------------------------------------

    @on(Select.Changed, "#ns-select")
    def on_namespace_selected(self, event: Select.Changed) -> None:
        if self._suspend_ns_select_handler:
            # Consume the one suppression here, not where it was set — the
            # posted Changed message this guards against may not have been
            # handled yet at the point the flag would otherwise be reset.
            self._suspend_ns_select_handler = False
            return
        if event.value is Select.BLANK:
            return
        ns_id = event.value
        owner_id = self._ns_owner.get(ns_id)
        if owner_id is None:
            return
        # A genuine namespace change picked via the dropdown always shows
        # everything in it — there's no UI left to narrow to one connector
        # (only DetailScreen's pre-scoped 'o' entry point does that).
        self._selected_obj = None
        self._load_selection_worker(ns_id, owner_id, group_id=None)

    @work(thread=False)
    async def _load_selection_worker(
        self, ns_id: uuid.UUID, owner_id: uuid.UUID, group_id: uuid.UUID | None
    ) -> None:
        await self._load_selection(ns_id, owner_id, group_id)

    # ------------------------------------------------------------------
    # Right-panel rebuilds
    # ------------------------------------------------------------------

    def _rebuild_filetree(self) -> None:
        tree = self.query_one("#objects-filetree", Tree)
        tree.clear()
        self._file_leaf_map.clear()
        if not self._files:
            return

        tree.root.set_label("/")
        tree.root.expand()

        dir_cache: dict[str, TreeNode] = {}

        def get_or_create_dir(path: str) -> TreeNode:
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

        for obj in self._files:
            p = PurePosixPath(obj.source)
            parent_str = str(p.parent)
            if parent_str in ("", ".", "/"):
                leaf = tree.root.add_leaf(p.name)
            else:
                parent_node = get_or_create_dir(parent_str)
                leaf = parent_node.add_leaf(p.name)
            self._file_leaf_map[leaf.id] = obj

    def _rebuild_tasks(self, selected_obj: _FileEntry | None) -> None:
        table = self.query_one("#objects-tasks", DataTable)
        table.clear()

        task_map: dict[uuid.UUID, IngestionTaskResponse] = {}
        for t in self._tasks:
            if t.obj_id is None:
                continue
            existing = task_map.get(t.obj_id)
            # created_at (never None) rather than completed_at (None while
            # running, Epic 22) — an in-flight task is always the most
            # recent one for its object, so this still picks it correctly.
            if existing is None or t.created_at > existing.created_at:
                task_map[t.obj_id] = t

        objects_to_show = [selected_obj] if selected_obj else self._files
        for obj in objects_to_show:
            name = obj.source.split("/")[-1]
            task = task_map.get(obj.id)
            if task:
                completed = (
                    task.completed_at.strftime("%Y-%m-%d %H:%M")
                    if task.completed_at is not None
                    else "—"
                )
                reason = task.failure_reason or "—"
                table.add_row(
                    name, task_status_markup(task.status), task.stage, completed, reason
                )
            else:
                table.add_row(name, "—", "—", "—", "—")

    # ------------------------------------------------------------------
    # File tree selection (drives the tasks table filter + delete target)
    # ------------------------------------------------------------------

    @on(Tree.NodeSelected, "#objects-filetree")
    def on_file_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        obj = self._file_leaf_map.get(node.id)
        if obj is None:
            # A directory node, not a file leaf — show every object again.
            self._selected_obj = None
            self._rebuild_tasks(selected_obj=None)
            return
        self._selected_obj = obj
        self._rebuild_tasks(selected_obj=obj)

    def action_delete_object(self) -> None:
        if self._selected_obj is None:
            self.notify("Select a file first.", severity="warning")
            return
        self._confirm_delete_object(self._selected_obj)

    def action_delete_namespace(self) -> None:
        if self._active_ns_id is None:
            self.notify("No namespace loaded.", severity="warning")
            return
        self._confirm_delete_namespace(self._active_ns_id, self._active_owner_id)

    @work(thread=False)
    async def _confirm_delete_object(self, obj: _FileEntry) -> None:
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
        self._files = [f for f in self._files if f.id != obj.id]
        self._selected_obj = None
        self._refresh_right()

    @work(thread=False)
    async def _confirm_delete_namespace(
        self, ns_id: uuid.UUID, owner_id: uuid.UUID
    ) -> None:
        from src.cli.tui.widgets.confirm import ConfirmScreen

        ns_source = next((s for s in self._sources if s.namespace_id == ns_id), None)
        ns_label = (
            ns_source.namespace_name or str(ns_id)[:8]
            if ns_source
            else str(ns_id)[:8]
        )
        connector_count = sum(1 for s in self._sources if s.namespace_id == ns_id)
        connector_word = "connector" if connector_count == 1 else "connectors"
        confirmed = await self.app.push_screen_wait(
            ConfirmScreen(
                f"Delete namespace '{ns_label}' "
                f"({connector_count} {connector_word}) and ALL its objects?"
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
        self._sources = [s for s in self._sources if s.namespace_id != ns_id]
        self._active_ns_id = None
        self._active_owner_id = None
        self._active_group_id = None
        self._files = []
        self._tasks = []
        self._selected_obj = None
        self._rebuild_ns_select()
        self._refresh_right()

    def action_refresh(self) -> None:
        self._load()

    def action_back(self) -> None:
        self.app.pop_screen()
