"""Pilot tests for CreateScreen."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from textual.app import App
from textual.widgets import Input, SelectionList

from src.cli.tui.screens.create import CreateScreen

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    def __init__(self, mock_ds: AsyncMock) -> None:
        super().__init__()
        self._mock_ds = mock_ds

    async def on_mount(self) -> None:
        with patch(
            "src.cli.tui.screens.create.DataSourcesClient", return_value=self._mock_ds
        ):
            await self.push_screen(CreateScreen(gateway_url=_GW, org_name="acme"))


@pytest.mark.asyncio
async def test_org_field_disabled(mock_ds: AsyncMock) -> None:
    async with _Host(mock_ds).run_test() as pilot:
        await pilot.pause()
        inp = pilot.app.screen.query_one("#inp-org", Input)
        assert inp.disabled is True
        assert inp.value == "acme"


@pytest.mark.asyncio
async def test_submit_blank_name_sends_none_for_auto_generation(
    mock_ds: AsyncMock,
) -> None:
    """Display Name is optional — a blank #inp-name must not block
    submission; the server auto-generates connector-<uuid4> when it sees
    display_name=None."""
    async with _Host(mock_ds).run_test() as pilot:
        await pilot.pause()
        pilot.app.screen.query_one("#inp-namespace", Input).value = "test-ns"
        pilot.app.screen.query_one("#inp-path", Input).value = "/data/docs"
        pilot.app.screen.query_one("#btn-submit").scroll_visible(animate=False)
        await pilot.pause()
        # #inp-name starts empty; submit without filling it.
        await pilot.click("#btn-submit")
        await pilot.pause()
        await pilot.pause()  # _submit worker completes

    mock_ds.create_source.assert_called_once()
    payload = mock_ds.create_source.call_args.args[0]
    assert payload.display_name is None


@pytest.mark.asyncio
async def test_submit_missing_namespace_stays_on_screen(mock_ds: AsyncMock) -> None:
    async with _Host(mock_ds).run_test() as pilot:
        await pilot.pause()
        pilot.app.screen.query_one("#inp-path", Input).value = "/data/docs"
        pilot.app.screen.query_one("#btn-submit").scroll_visible(animate=False)
        await pilot.pause()
        # #inp-namespace starts empty; submit without filling it.
        await pilot.click("#btn-submit")
        await pilot.pause()
        assert isinstance(pilot.app.screen, CreateScreen)
    mock_ds.create_source.assert_not_called()


@pytest.mark.asyncio
async def test_submit_calls_create_source(mock_ds: AsyncMock) -> None:
    async with _Host(mock_ds).run_test() as pilot:
        await pilot.pause()
        pilot.app.screen.query_one("#inp-name", Input).value = "test-watcher"
        pilot.app.screen.query_one("#inp-namespace", Input).value = "test-ns"
        pilot.app.screen.query_one("#inp-path", Input).value = "/data/docs"
        pilot.app.screen.query_one("#btn-submit").scroll_visible(animate=False)
        await pilot.pause()
        await pilot.click("#btn-submit")
        await pilot.pause()
        await pilot.pause()  # _submit worker completes
    mock_ds.create_source.assert_called_once()


@pytest.mark.asyncio
async def test_submit_no_file_types_selected_stays_on_screen(
    mock_ds: AsyncMock,
) -> None:
    async with _Host(mock_ds).run_test() as pilot:
        await pilot.pause()
        pilot.app.screen.query_one("#inp-name", Input).value = "test-watcher"
        pilot.app.screen.query_one("#inp-namespace", Input).value = "test-ns"
        pilot.app.screen.query_one("#inp-path", Input).value = "/data/docs"
        pilot.app.screen.query_one("#inp-file-extensions", SelectionList).deselect_all()
        pilot.app.screen.query_one("#btn-submit").scroll_visible(animate=False)
        await pilot.pause()
        await pilot.click("#btn-submit")
        await pilot.pause()
        assert isinstance(pilot.app.screen, CreateScreen)
    mock_ds.create_source.assert_not_called()


@pytest.mark.asyncio
async def test_submit_failure_notifies_and_stays_on_screen(mock_ds: AsyncMock) -> None:
    mock_ds.create_source.side_effect = Exception("boom")
    async with _Host(mock_ds).run_test() as pilot:
        await pilot.pause()
        pilot.app.screen.query_one("#inp-name", Input).value = "test-watcher"
        pilot.app.screen.query_one("#inp-namespace", Input).value = "test-ns"
        pilot.app.screen.query_one("#inp-path", Input).value = "/data/docs"
        pilot.app.screen.query_one("#btn-submit").scroll_visible(animate=False)
        await pilot.pause()
        await pilot.click("#btn-submit")
        await pilot.pause()
        await pilot.pause()  # _submit worker completes
        assert isinstance(pilot.app.screen, CreateScreen)
    mock_ds.create_source.assert_called_once()
