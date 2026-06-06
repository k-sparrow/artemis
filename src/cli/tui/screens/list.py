"""ListScreen: data sources grouped by namespace in a Tree view."""

from __future__ import annotations

from collections import defaultdict

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, Footer, Header, Label, Tree
from textual.widgets.tree import TreeNode

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.cli.client import DataSourcesClient
from src.cli.tui.widgets.status_badge import status_markup


class ListScreen(Screen):
    """Main screen: data sources grouped by namespace."""

    BINDINGS = [
        ("n", "new_source", "New"),
        ("d", "delete_source", "Delete"),
        ("r", "refresh", "Refresh"),
        ("q", "quit", "Quit"),
        ("enter", "open_detail", "Detail"),
    ]

    def __init__(self, gateway_url: str | None = None) -> None:
        super().__init__()
        self._client = DataSourcesClient(base_url=gateway_url)
        self._gateway_url = gateway_url
        self._sources: list[DataSourceResponse] = []
        # Maps tree node id → DataSourceResponse
        self._node_map: dict[int, DataSourceResponse] = {}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Tree("Data Sources", id="source-tree")
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Artemis Data Sources"
        self._load_sources()
        self.set_interval(10, self._load_sources)

    @work(thread=False)
    async def _load_sources(self) -> None:
        try:
            self._sources = await self._client.list_sources()
        except Exception as exc:
            self.notify(f"Error loading sources: {exc}", severity="error")
            return
        self._rebuild_tree()

    def _rebuild_tree(self) -> None:
        tree = self.query_one("#source-tree", Tree)
        tree.clear()
        self._node_map.clear()

        # Group by namespace_name (fall back to first 8 chars of namespace_id)
        by_ns: dict[str, list[DataSourceResponse]] = defaultdict(list)
        for source in self._sources:
            ns_label = source.namespace_name or str(source.namespace_id)[:8]
            by_ns[ns_label].append(source)

        for ns_label in sorted(by_ns):
            ns_node: TreeNode = tree.root.add(ns_label, expand=True)
            for source in sorted(by_ns[ns_label], key=lambda s: s.display_name):
                state = source.kafka_status.state if source.kafka_status else None
                badge = status_markup(state)
                label = (
                    f"{source.display_name:<24} {source.source_type:<12} "
                    f"{badge}  {source.created_at.strftime('%Y-%m-%d')}"
                )
                leaf = ns_node.add_leaf(label)
                self._node_map[leaf.id] = source

        tree.root.expand()

    def _selected_source(self) -> DataSourceResponse | None:
        tree = self.query_one("#source-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return None
        return self._node_map.get(node.id)

    @on(Tree.NodeSelected)
    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        source = self._node_map.get(event.node.id)
        if source is not None:
            from src.cli.tui.screens.detail import DetailScreen

            self.app.push_screen(DetailScreen(source.id, gateway_url=self._gateway_url))

    def action_open_detail(self) -> None:
        source = self._selected_source()
        if source:
            from src.cli.tui.screens.detail import DetailScreen

            self.app.push_screen(DetailScreen(source.id, gateway_url=self._gateway_url))

    def action_new_source(self) -> None:
        from src.cli.tui.screens.create import CreateScreen

        self.app.push_screen(CreateScreen(gateway_url=self._gateway_url))

    def action_delete_source(self) -> None:
        source = self._selected_source()
        if source is None:
            self.notify("Select a data source first.", severity="warning")
            return
        self._confirm_delete(source)

    @work(thread=False)
    async def _confirm_delete(self, source: DataSourceResponse) -> None:
        confirmed = await self.app.push_screen_wait(
            _ConfirmScreen(f"Delete '{source.display_name}'?")
        )
        if confirmed:
            try:
                await self._client.delete_source(source.id)
                self.notify(f"Deleted '{source.display_name}'.")
            except Exception as exc:
                self.notify(f"Delete failed: {exc}", severity="error")
            self._load_sources()

    def action_refresh(self) -> None:
        self._load_sources()

    def action_quit(self) -> None:
        self.app.exit()


# ---------------------------------------------------------------------------
# Inline confirmation modal
# ---------------------------------------------------------------------------


class _ConfirmScreen(ModalScreen[bool]):
    """Simple yes/no modal."""

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, message: str) -> None:
        super().__init__()
        self._message = message

    def compose(self) -> ComposeResult:
        yield Label(self._message)
        yield Horizontal(
            Button("Yes", id="yes", variant="error"),
            Button("No", id="no"),
        )

    @on(Button.Pressed, "#yes")
    def on_yes(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#no")
    def on_no(self) -> None:
        self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)
