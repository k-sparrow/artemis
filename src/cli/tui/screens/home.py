"""HomeScreen: welcoming root screen with metrics and last-ingested sidebar."""

from __future__ import annotations

import asyncio
from collections import defaultdict

from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.cli.client import DataSourcesClient, StorageClient
from src.cli.settings import settings


class HomeScreen(Screen):
    """Root screen: headline metrics on the left, last-ingested sidebar on the right."""

    BINDINGS = [
        ("s", "connectors", "Sources"),
        ("n", "new_source", "New"),
        ("h", "health", "Health"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, gateway_url: str | None = None, org_name: str = "") -> None:
        super().__init__()
        self._gateway_url = gateway_url
        self._org_name = org_name
        self._ds_client = DataSourcesClient(base_url=gateway_url)
        self._storage_client = StorageClient(base_url=gateway_url)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Label(
                "A R T E M I S   S O U R C E S",
                id="home-title",
            ),
            Label(
                f"Gateway  {settings.GATEWAY_URL}",
                id="home-gateway",
            ),
            Horizontal(
                DataTable(id="home-metrics", show_cursor=False),
                DataTable(id="home-sidebar", show_cursor=False),
                id="home-tables",
            ),
            id="home-content",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Artemis Sources"
        metrics = self.query_one("#home-metrics", DataTable)
        metrics.add_columns("Metric", "Value")

        sidebar = self.query_one("#home-sidebar", DataTable)
        sidebar.add_columns("Org", "Namespace", "Object", "Ingested At")

        self._load()
        self.set_interval(30, self._load)

    @work(thread=False)
    async def _load(self) -> None:
        try:
            sources = await self._ds_client.list_sources()
        except Exception as exc:
            self.notify(f"Error loading sources: {exc}", severity="error")
            return

        self._update_metrics(sources)
        await self._update_sidebar(sources)

    def _update_metrics(self, sources: list[DataSourceResponse]) -> None:
        metrics = self.query_one("#home-metrics", DataTable)
        metrics.clear()

        by_ns: dict[str, list[DataSourceResponse]] = defaultdict(list)
        for s in sources:
            ns = s.namespace_name or str(s.namespace_id)[:8]
            by_ns[ns].append(s)

        state_counts: dict[str, int] = defaultdict(int)
        for s in sources:
            state = (s.kafka_status.state if s.kafka_status else "UNKNOWN").upper()
            state_counts[state] += 1

        last = max(sources, key=lambda s: s.created_at, default=None)

        metrics.add_row("Namespaces", str(len(by_ns)))
        metrics.add_row("Connectors", str(len(sources)))
        for state, count in sorted(state_counts.items()):
            metrics.add_row(state.capitalize(), str(count))
        if last:
            metrics.add_row("Last Created", "")
            metrics.add_row(
                f"  {last.display_name}", last.created_at.strftime("%Y-%m-%d")
            )

    async def _update_sidebar(self, sources: list[DataSourceResponse]) -> None:
        # Collect unique namespaces with one representative source per namespace.
        ns_map: dict[str, DataSourceResponse] = {}
        for s in sources:
            ns_key = str(s.namespace_id)
            if ns_key not in ns_map:
                ns_map[ns_key] = s

        async def _fetch_recent(s: DataSourceResponse):
            org = s.org_name
            ns_label = s.namespace_name or str(s.namespace_id)[:8]
            try:
                objects = await self._storage_client.list_objects(
                    s.namespace_id, s.owner_id, limit=10, order="desc"
                )
                return org, ns_label, objects
            except Exception:
                return org, ns_label, []

        results = await asyncio.gather(*[_fetch_recent(s) for s in ns_map.values()])

        sidebar = self.query_one("#home-sidebar", DataTable)
        sidebar.clear()
        for org, ns_label, objects in results:
            # Skip namespaces that have no ingested objects yet.
            if not objects:
                continue
            for obj in objects:
                when = obj.ingested_at.strftime("%Y-%m-%d %H:%M")
                name = obj.source.split("/")[-1]
                sidebar.add_row(org, ns_label, name, when)

    def action_connectors(self) -> None:
        from src.cli.tui.screens.connectors import ConnectorsScreen

        self.app.push_screen(
            ConnectorsScreen(gateway_url=self._gateway_url, org_name=self._org_name)
        )

    def action_new_source(self) -> None:
        from src.cli.tui.screens.create import CreateScreen

        self.app.push_screen(
            CreateScreen(gateway_url=self._gateway_url, org_name=self._org_name)
        )

    def action_health(self) -> None:
        from src.cli.tui.screens.health import HealthScreen

        self.app.push_screen(HealthScreen(gateway_url=self._gateway_url))

    def action_quit(self) -> None:
        self.app.exit()
