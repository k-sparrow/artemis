"""HomeScreen: root screen with headline metrics and a live activity feed."""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.backend.storage.api.files.schemas import IngestionTaskResponse
from src.cli.client import DataSourcesClient, StorageClient
from src.cli.settings import settings
from src.cli.tui.widgets.status_badge import task_status_markup

# Per-namespace candidate pool for the activity fan-out (Epic 16.5) —
# ingestion_status keeps one permanent row per object ever ingested, so this
# must stay bounded; list_tasks has its own server-side limit/order to match.
_ACTIVITY_FETCH_LIMIT_PER_NAMESPACE = 10
# What's actually shown: a small "latest activity" window merged across
# every namespace, not up to 10 rows per namespace stacked on top of each
# other — Home is a glance, not a full activity log (that's ObjectsScreen's
# job, scoped to one namespace with much more room).
_ACTIVITY_DISPLAY_LIMIT = 8


class HomeScreen(Screen):
    """Root screen: headline metrics on the left, live task activity on the right."""

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
        # Parallel to #home-activity's rows — lets RowSelected navigate to
        # the right namespace without re-deriving it from displayed text.
        self._activity_rows: list[tuple[uuid.UUID, uuid.UUID]] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Label(
                "A R T E M I S",
                id="home-title",
            ),
            Label(
                f"Gateway  {settings.GATEWAY_URL}",
                id="home-gateway",
            ),
            Horizontal(
                DataTable(id="home-metrics", show_cursor=False),
                DataTable(id="home-activity", cursor_type="row"),
                id="home-tables",
            ),
            id="home-content",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Artemis"
        metrics = self.query_one("#home-metrics", DataTable)
        metrics.add_columns("Metric", "Value")

        activity = self.query_one("#home-activity", DataTable)
        activity.add_columns("Namespace", "Object", "Status", "Stage", "Since")

        self._load()
        # Tighter than the old 30s sidebar cadence — task stage changes
        # faster than "a new object finished ingesting."
        self.set_interval(10, self._load)

    @work(thread=False)
    async def _load(self) -> None:
        try:
            sources = await self._ds_client.list_sources()
        except Exception as exc:
            self.notify(f"Error loading sources: {exc}", severity="error")
            return

        self._update_metrics(sources)
        await self._update_activity(sources)

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

    async def _update_activity(self, sources: list[DataSourceResponse]) -> None:
        """Epic 16.5: cross-namespace live task activity — the actual point
        of Epic 22's ``stage`` field, surfaced on the very first screen
        instead of buried 2 hops away with no auto-refresh. Same
        one-fetch-per-namespace fan-out shape the old object sidebar used,
        just calling list_tasks (bounded server-side) instead.

        Deliberately a small "latest activity" glance, not a full log: each
        namespace contributes its own recent-task pool, but the table only
        ever shows the _ACTIVITY_DISPLAY_LIMIT most recent overall, merged
        and re-sorted — full per-namespace history belongs on ObjectsScreen.
        """
        # Collect unique namespaces with one representative source per namespace.
        ns_map: dict[str, DataSourceResponse] = {}
        for s in sources:
            ns_key = str(s.namespace_id)
            if ns_key not in ns_map:
                ns_map[ns_key] = s

        async def _fetch_tasks(s: DataSourceResponse):
            org = s.org_name
            ns_label = s.namespace_name or str(s.namespace_id)[:8]
            try:
                tasks = await self._storage_client.list_tasks(
                    s.namespace_id,
                    s.owner_id,
                    limit=_ACTIVITY_FETCH_LIMIT_PER_NAMESPACE,
                    order="desc",
                )
                return org, ns_label, s.namespace_id, s.owner_id, tasks
            except Exception:
                return org, ns_label, s.namespace_id, s.owner_id, []

        results = await asyncio.gather(*[_fetch_tasks(s) for s in ns_map.values()])

        # Flatten every namespace's candidates into one pool, then keep only
        # the most recent handful overall — "since" (completed_at, or
        # created_at while still running) is both what's displayed and what
        # ranks recency here, so the two never disagree.
        pool: list[tuple[str, str, uuid.UUID, uuid.UUID, IngestionTaskResponse]] = [
            (org, ns_label, ns_id, owner_id, task)
            for org, ns_label, ns_id, owner_id, tasks in results
            for task in tasks
        ]
        pool.sort(key=lambda row: row[4].completed_at or row[4].created_at, reverse=True)
        latest = pool[:_ACTIVITY_DISPLAY_LIMIT]

        activity = self.query_one("#home-activity", DataTable)
        activity.clear()
        self._activity_rows = []
        for org, ns_label, ns_id, owner_id, task in latest:
            self._add_activity_row(org, ns_label, task)
            self._activity_rows.append((ns_id, owner_id))

    def _add_activity_row(
        self, org: str, ns_label: str, task: IngestionTaskResponse
    ) -> None:
        activity = self.query_one("#home-activity", DataTable)
        name = task.source.split("/")[-1] if task.source else str(task.obj_id)[:8]
        since = (task.completed_at or task.created_at).strftime("%Y-%m-%d %H:%M")
        activity.add_row(
            f"{ns_label} ({org})",
            name,
            task_status_markup(task.status),
            task.stage,
            since,
        )

    @on(DataTable.RowSelected, "#home-activity")
    def on_activity_row_selected(self, event: DataTable.RowSelected) -> None:
        idx = event.cursor_row
        if not (0 <= idx < len(self._activity_rows)):
            return
        ns_id, _owner_id = self._activity_rows[idx]
        from src.cli.tui.screens.objects import ObjectsScreen

        self.app.push_screen(
            ObjectsScreen(gateway_url=self._gateway_url, namespace_id=ns_id)
        )

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
