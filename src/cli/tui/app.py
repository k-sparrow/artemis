"""Artemis Data Sources TUI application."""

from __future__ import annotations

from pathlib import Path

from textual.app import App

from src.cli.tui.screens.home import HomeScreen

_CSS_PATH = Path(__file__).parent / "artemis.tcss"


class DataSourcesApp(App):
    """Artemis data source management TUI."""

    TITLE = "Artemis Sources"
    DARK = True
    CSS_PATH = _CSS_PATH
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self, gateway_url: str | None = None, org_name: str = "") -> None:
        super().__init__()
        self._gateway_url = gateway_url
        self._org_name = org_name

    def on_mount(self) -> None:
        self.push_screen(
            HomeScreen(gateway_url=self._gateway_url, org_name=self._org_name)
        )
