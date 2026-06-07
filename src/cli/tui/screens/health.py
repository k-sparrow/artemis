"""HealthScreen: per-service readiness probe status table."""

from __future__ import annotations

import asyncio
import time

import httpx
from textual import work
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header

from src.cli.settings import settings

# Services probed directly (not through the gateway, which doesn't route /health/*).
# Each service exposes GET /health/readiness on its own base URL.
# Gateway liveness is probed via APISIX's own endpoint.
_SERVICES = [
    ("Gateway", "{gateway}/health/liveness"),
    ("Storage", "http://localhost:7000/health/readiness"),
    ("Data Sources", "http://localhost:9500/health/readiness"),
    ("Indexing", "http://localhost:8000/health/readiness"),
]


class HealthScreen(Screen):
    """Per-service health check table, auto-refreshes every 10 s."""

    BINDINGS = [
        ("b", "back", "Back"),
        ("escape", "back", "Back"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, gateway_url: str | None = None) -> None:
        super().__init__()
        self._gateway_url = gateway_url or settings.GATEWAY_URL

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield DataTable(id="health-table", show_cursor=False)
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Service Health"
        table = self.query_one("#health-table", DataTable)
        table.add_columns("Service", "Status", "Latency")
        self._check_all()
        self.set_interval(10, self._check_all)

    @work(thread=False)
    async def _check_all(self) -> None:
        services = [
            (name, url.replace("{gateway}", self._gateway_url))
            for name, url in _SERVICES
        ]

        async def _probe(name: str, url: str) -> tuple[str, str, str]:
            t0 = time.monotonic()
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(url)
                latency_ms = int((time.monotonic() - t0) * 1000)
                if resp.status_code < 400:
                    return name, "[green]● OK[/green]", f"{latency_ms} ms"
                return name, "[red]✗ FAIL[/red]", f"{latency_ms} ms"
            except Exception:
                return name, "[red]✗ FAIL[/red]", "—"

        results = await asyncio.gather(*[_probe(n, u) for n, u in services])

        table = self.query_one("#health-table", DataTable)
        table.clear()
        for name, status, latency in results:
            table.add_row(name, status, latency)

    def action_refresh(self) -> None:
        self._check_all()

    def action_back(self) -> None:
        self.app.pop_screen()
