"""CreateScreen: form to register a new filesystem data source."""

from __future__ import annotations

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Button, Checkbox, Footer, Header, Input, Label

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceCreate
from src.cli.client import DataSourcesClient


class CreateScreen(Screen):
    """Form screen for creating a new filesystem data source."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]

    def __init__(self, gateway_url: str | None = None) -> None:
        super().__init__()
        self._client = DataSourcesClient(base_url=gateway_url)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Label("Create Data Source", id="form-title"),
            Label("Display Name"),
            Input(placeholder="e.g. docs-watcher", id="inp-name"),
            Label("Namespace"),
            Input(placeholder="e.g. docs-namespace", id="inp-namespace"),
            Label("Org Name"),
            Input(placeholder="e.g. acme-corp", id="inp-org"),
            Label("Path"),
            Input(placeholder="/data/documents", id="inp-path"),
            Checkbox("Recursive", value=True, id="inp-recursive"),
            Button("Create", id="btn-submit", variant="success"),
            Button("Cancel", id="btn-cancel"),
            id="form",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "New Data Source"
        self.query_one("#inp-name", Input).focus()

    @on(Button.Pressed, "#btn-submit")
    def on_submit(self) -> None:
        self._submit()

    @on(Button.Pressed, "#btn-cancel")
    def on_cancel_btn(self) -> None:
        self.action_cancel()

    def action_cancel(self) -> None:
        self.app.pop_screen()

    @work(thread=False)
    async def _submit(self) -> None:
        name = self.query_one("#inp-name", Input).value.strip()
        namespace = self.query_one("#inp-namespace", Input).value.strip()
        org = self.query_one("#inp-org", Input).value.strip()
        path = self.query_one("#inp-path", Input).value.strip()
        recursive = self.query_one("#inp-recursive", Checkbox).value

        if not all([name, namespace, org, path]):
            self.notify("All fields are required.", severity="warning")
            return

        payload = DataSourceCreate(
            display_name=name,
            namespace=namespace,
            org_name=org,
            path=path,
            recursive=recursive,
        )

        try:
            source = await self._client.create_source(payload)
            self.notify(f"Created '{source.display_name}' ({str(source.id)[:8]})")
            self.app.pop_screen()
        except Exception as exc:
            self.notify(f"Create failed: {exc}", severity="error")
