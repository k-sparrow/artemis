"""Artemis Data Sources TUI application."""

from __future__ import annotations

from pathlib import Path

from textual.app import App

from src.cli.tui.screens.list import ListScreen

_CSS_PATH = Path(__file__).parent / "artemis.tcss"


class DataSourcesApp(App):
    """Artemis data source management TUI."""

    TITLE = "Artemis Data Sources"
    DARK = True
    CSS_PATH = _CSS_PATH
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self, gateway_url: str | None = None) -> None:
        super().__init__()
        self._gateway_url = gateway_url

    def on_mount(self) -> None:
        self.push_screen(ListScreen(gateway_url=self._gateway_url))
