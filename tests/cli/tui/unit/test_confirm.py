"""Pilot tests for ConfirmScreen modal."""

from __future__ import annotations

import pytest
from textual import work
from textual.app import App

from src.cli.tui.widgets.confirm import ConfirmScreen


class _Host(App):
    CSS = ""

    def on_mount(self) -> None:
        self._result: bool | None = None
        self._push_confirm()

    @work(thread=False)
    async def _push_confirm(self) -> None:
        self._result = await self.push_screen_wait(ConfirmScreen("Delete item?"))


@pytest.mark.asyncio
async def test_yes_returns_true() -> None:
    app = _Host()
    async with app.run_test() as pilot:
        await pilot.pause()  # worker starts, ConfirmScreen pushed
        await pilot.click("#yes")
        await pilot.pause()  # ConfirmScreen dismissed, _result set
    assert app._result is True


@pytest.mark.asyncio
async def test_no_returns_false() -> None:
    app = _Host()
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.click("#no")
        await pilot.pause()
    assert app._result is False


@pytest.mark.asyncio
async def test_escape_returns_false() -> None:
    app = _Host()
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("escape")
        await pilot.pause()
    assert app._result is False
