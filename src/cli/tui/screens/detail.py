"""DetailScreen: single data source with config, tasks, and action buttons."""

from __future__ import annotations

import uuid

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Label, Static

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceResponse
from src.cli.client import DataSourcesClient
from src.cli.tui.widgets.status_badge import status_markup


class DetailScreen(Screen):
    """Detailed view of a single data source with lifecycle action buttons."""

    BINDINGS = [
        ("p", "pause", "Pause"),
        ("u", "resume", "Resume"),
        ("r", "restart", "Restart"),
        ("d", "delete", "Delete"),
        ("b", "back", "Back"),
        ("escape", "back", "Back"),
    ]

    def __init__(self, source_id: uuid.UUID, gateway_url: str | None = None) -> None:
        super().__init__()
        self._source_id = source_id
        self._client = DataSourcesClient(base_url=gateway_url)
        self._source: DataSourceResponse | None = None

    # Textual 8.2.7: Screen._render() can return None for pushed screens via
    # certain code paths, crashing Visual.to_strips. BLANK = True makes
    # render_line return Strip.blank() early, skipping _render_content entirely.
    # Safe here because all visible content is rendered by compose() children.
    BLANK = True

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Static(id="info"),
            Static(id="tasks"),
            Horizontal(
                Button("Pause [P]", id="btn-pause", variant="warning"),
                Button("Resume [U]", id="btn-resume", variant="success"),
                Button("Restart [R]", id="btn-restart"),
                Button("Delete [D]", id="btn-delete", variant="error"),
                Button("Back [B]", id="btn-back"),
                id="actions",
            ),
        )
        yield Footer()

    def on_mount(self) -> None:
        self._load()

    @work(thread=False)
    async def _load(self) -> None:
        try:
            self._source = await self._client.get_source(self._source_id)
        except Exception as exc:
            self.notify(f"Error loading source: {exc}", severity="error")
            return
        self._render()

    def _render(self) -> None:
        s = self._source
        if s is None:
            return

        state = s.kafka_status.state if s.kafka_status else None
        worker = s.kafka_status.worker_id if s.kafka_status else "—"
        ns = s.namespace_name or str(s.namespace_id)[:8]
        path = s.config.get("path", "—")
        recursive = "Yes" if s.config.get("recursive", True) else "No"

        self.title = f"Data Source: {s.display_name}"
        self.query_one("#info", Static).update(
            f"Namespace:    {ns}\n"
            f"Org:          {s.org_name}\n"
            f"Type:         {s.source_type}\n"
            f"Path:         {path}\n"
            f"Recursive:    {recursive}\n"
            f"Created:      {s.created_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"Connector:    {s.connector_name}\n"
            f"Status:       {status_markup(state)}\n"
            f"Worker:       {worker}"
        )

        tasks_text = "Tasks\n"
        if s.kafka_status and s.kafka_status.tasks:
            for t in s.kafka_status.tasks:
                trace = f"  trace: {t.trace}" if t.trace else ""
                tasks_text += f"  #{t.id}  {status_markup(t.state)}  {t.worker_id}{trace}\n"
        else:
            tasks_text += "  (none)"
        self.query_one("#tasks", Static).update(tasks_text)

    # ------------------------------------------------------------------
    # Button events
    # ------------------------------------------------------------------

    @on(Button.Pressed, "#btn-pause")
    def on_pause(self) -> None:
        self.action_pause()

    @on(Button.Pressed, "#btn-resume")
    def on_resume(self) -> None:
        self.action_resume()

    @on(Button.Pressed, "#btn-restart")
    def on_restart(self) -> None:
        self.action_restart()

    @on(Button.Pressed, "#btn-delete")
    def on_delete(self) -> None:
        self.action_delete()

    @on(Button.Pressed, "#btn-back")
    def on_back(self) -> None:
        self.action_back()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_pause(self) -> None:
        self._do_action("pause")

    def action_resume(self) -> None:
        self._do_action("resume")

    def action_restart(self) -> None:
        self._do_action("restart")

    def action_back(self) -> None:
        self.app.pop_screen()

    def action_delete(self) -> None:
        self._confirm_delete()

    @work(thread=False)
    async def _confirm_delete(self) -> None:
        from src.cli.tui.screens.list import _ConfirmScreen
        name = self._source.display_name if self._source else str(self._source_id)
        confirmed = await self.app.push_screen_wait(_ConfirmScreen(f"Delete '{name}'?"))
        if confirmed:
            try:
                await self._client.delete_source(self._source_id)
                self.notify(f"Deleted '{name}'.")
                self.app.pop_screen()
            except Exception as exc:
                self.notify(f"Delete failed: {exc}", severity="error")

    @work(thread=False)
    async def _do_action(self, action: str) -> None:
        try:
            match action:
                case "pause":
                    self._source = await self._client.pause_source(self._source_id)
                case "resume":
                    self._source = await self._client.resume_source(self._source_id)
                case "restart":
                    self._source = await self._client.restart_source(self._source_id)
            self._render()
            self.notify(f"{action.capitalize()} successful.")
        except Exception as exc:
            self.notify(f"{action.capitalize()} failed: {exc}", severity="error")
