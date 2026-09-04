"""ConnectorsScreen: data sources in tree or table view with filtering."""

from __future__ import annotations

from collections import defaultdict

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Input, Label, Tree
from textual.widgets.tree import TreeNode

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.cli.client import DataSourcesClient
from src.cli.tui.widgets.status_badge import status_markup


class ConnectorsScreen(Screen):
    """Data sources in tree (default) or table view; filterable with /."""

    BINDINGS = [
        ("t", "toggle_view", "Toggle View"),
        ("/", "filter", "Filter"),
        ("n", "new_source", "New"),
        ("o", "objects", "Objects"),
        ("d", "delete_source", "Delete"),
        ("r", "refresh", "Refresh"),
        ("b", "back", "Back"),
        ("escape", "back", "Back"),
        ("enter", "open_detail", "Detail"),
    ]

    def __init__(self, gateway_url: str | None = None, org_name: str = "") -> None:
        super().__init__()
        self._client = DataSourcesClient(base_url=gateway_url)
        self._gateway_url = gateway_url
        self._org_name = org_name
        self._sources: list[DataSourceResponse] = []
        self._node_map: dict[int, DataSourceResponse] = {}
        self._table_rows: list[DataSourceResponse] = []  # mirrors table row order
        self._tree_mode = True
        self._filter_text = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Input(placeholder="Filter…", id="filter-input"),
            Tree("Data Sources", id="source-tree"),
            DataTable(id="source-table"),
            Label("No data sources found.", id="empty-label"),
            id="connectors-body",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Connectors"
        self.query_one("#filter-input").display = False
        self.query_one("#source-table", DataTable).display = False
        self.query_one("#empty-label", Label).display = False

        table = self.query_one("#source-table", DataTable)
        table.add_columns("Namespace", "Org", "Name", "Type", "Status", "Created")

        # Textual gives initial focus to the first focusable widget in
        # compose() order, which is #filter-input — even though it's hidden
        # right above. Left alone, every single keybinding on this screen
        # (d/o/n/t/r, even '/' itself) silently types into the invisible
        # filter box instead of reaching any action. Move focus to the tree,
        # the actual primary navigable widget.
        self.query_one("#source-tree", Tree).focus()

        self._load_sources()
        self.set_interval(10, self._load_sources)

    @work(thread=False)
    async def _load_sources(self) -> None:
        try:
            self._sources = await self._client.list_sources()
        except Exception as exc:
            self.notify(f"Error loading sources: {exc}", severity="error")
            return
        self._rebuild()

    def _filtered(self) -> list[DataSourceResponse]:
        if not self._filter_text:
            return self._sources
        q = self._filter_text.lower()
        return [
            s
            for s in self._sources
            if q in (s.namespace_name or "").lower()
            or q in s.display_name.lower()
            or q in s.org_name.lower()
            or q in s.source_type.lower()
            or q in (s.kafka_status.state if s.kafka_status else "").lower()
        ]

    def _rebuild(self) -> None:
        if self._tree_mode:
            self._rebuild_tree()
        else:
            self._rebuild_table()

    def _rebuild_tree(self) -> None:
        tree = self.query_one("#source-tree", Tree)
        tree.display = True
        self.query_one("#source-table", DataTable).display = False

        tree.clear()
        self._node_map.clear()

        sources = self._filtered()

        by_ns: dict[str, list[DataSourceResponse]] = defaultdict(list)
        for s in sources:
            ns_label = s.namespace_name or str(s.namespace_id)[:8]
            by_ns[ns_label].append(s)

        if not by_ns:
            self.query_one("#empty-label", Label).display = True
            tree.display = False
            return

        self.query_one("#empty-label", Label).display = False

        for ns_label in sorted(by_ns):
            ns_node: TreeNode = tree.root.add(ns_label, expand=True)
            for s in sorted(by_ns[ns_label], key=lambda x: x.display_name):
                state = s.kafka_status.state if s.kafka_status else None
                badge = status_markup(state)
                label = (
                    f"{s.display_name:<24} {s.source_type:<12} "
                    f"{badge}  {s.created_at.strftime('%Y-%m-%d')}"
                )
                leaf = ns_node.add_leaf(label)
                self._node_map[leaf.id] = s

        tree.root.expand()

    def _rebuild_table(self) -> None:
        self.query_one("#source-tree", Tree).display = False
        table = self.query_one("#source-table", DataTable)
        table.display = True
        table.clear()

        sources = self._filtered()
        if not sources:
            self.query_one("#empty-label", Label).display = True
            table.display = False
            return

        self.query_one("#empty-label", Label).display = False
        ordered = sorted(
            sources, key=lambda x: (x.namespace_name or "", x.display_name)
        )
        self._table_rows = ordered
        for s in ordered:
            ns = s.namespace_name or str(s.namespace_id)[:8]
            state = s.kafka_status.state if s.kafka_status else "UNKNOWN"
            table.add_row(
                ns,
                s.org_name,
                s.display_name,
                s.source_type,
                status_markup(state),
                s.created_at.strftime("%Y-%m-%d"),
            )

    def _selected_source(self) -> DataSourceResponse | None:
        if self._tree_mode:
            tree = self.query_one("#source-tree", Tree)
            node = tree.cursor_node
            if node is None:
                return None
            return self._node_map.get(node.id)
        else:
            table = self.query_one("#source-table", DataTable)
            idx = table.cursor_row
            if 0 <= idx < len(self._table_rows):
                return self._table_rows[idx]
            return None

    @on(Tree.NodeSelected)
    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        source = self._node_map.get(event.node.id)
        if source is not None:
            from src.cli.tui.screens.detail import DetailScreen

            self.app.push_screen(DetailScreen(source.id, gateway_url=self._gateway_url))

    @on(Input.Changed, "#filter-input")
    def on_filter_changed(self, event: Input.Changed) -> None:
        self._filter_text = event.value
        self._rebuild()

    @on(Input.Submitted, "#filter-input")
    def on_filter_submitted(self) -> None:
        self._dismiss_filter()

    def on_key(self, event) -> None:
        if event.key == "escape":
            fi = self.query_one("#filter-input", Input)
            if fi.display and fi.has_focus:
                self._dismiss_filter()
                event.stop()

    def _dismiss_filter(self) -> None:
        fi = self.query_one("#filter-input", Input)
        fi.display = False
        self.set_focus(None)

    def action_toggle_view(self) -> None:
        self._tree_mode = not self._tree_mode
        label = "Tree" if self._tree_mode else "Table"
        self.notify(f"Switched to {label} view.")
        self._rebuild()

    def action_filter(self) -> None:
        fi = self.query_one("#filter-input", Input)
        fi.display = True
        fi.focus()

    def action_open_detail(self) -> None:
        source = self._selected_source()
        if source:
            from src.cli.tui.screens.detail import DetailScreen

            self.app.push_screen(DetailScreen(source.id, gateway_url=self._gateway_url))

    def action_objects(self) -> None:
        from src.cli.tui.screens.objects import ObjectsScreen

        self.app.push_screen(ObjectsScreen(gateway_url=self._gateway_url))

    def action_delete_source(self) -> None:
        source = self._selected_source()
        if source is None:
            self.notify("Select a connector first.", severity="warning")
            return
        self._confirm_delete(source)

    @work(thread=False)
    async def _confirm_delete(self, source: DataSourceResponse) -> None:
        from src.cli.tui.widgets.confirm import ConfirmScreen

        confirmed = await self.app.push_screen_wait(
            ConfirmScreen(f"Delete '{source.display_name}'?")
        )
        if confirmed:
            try:
                await self._client.delete_source(source.id)
                self.notify(f"Deleted '{source.display_name}'.")
            except Exception as exc:
                self.notify(f"Delete failed: {exc}", severity="error")
            self._load_sources()

    def action_new_source(self) -> None:
        from src.cli.tui.screens.create import CreateScreen

        self.app.push_screen(
            CreateScreen(gateway_url=self._gateway_url, org_name=self._org_name)
        )

    def action_refresh(self) -> None:
        self._load_sources()

    def action_back(self) -> None:
        self.app.pop_screen()
