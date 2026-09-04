"""Pilot tests for HealthScreen (previously zero coverage)."""

from __future__ import annotations

import pytest
import respx
from httpx import ConnectError, Response
from textual.app import App
from textual.widgets import DataTable

from src.cli.tui.screens.health import HealthScreen

_GW = "http://test:9080"


class _Host(App):
    CSS = ""

    async def on_mount(self) -> None:
        await self.push_screen(HealthScreen(gateway_url=_GW))


def _mock_all(mock: respx.MockRouter, *, storage=200, data_sources=200, indexing=200, gateway=200) -> None:
    mock.get(f"{_GW}/health/liveness").mock(return_value=Response(gateway))
    mock.get("http://localhost:7000/health/readiness").mock(
        return_value=Response(storage)
    )
    mock.get("http://localhost:9500/health/readiness").mock(
        return_value=Response(data_sources)
    )
    mock.get("http://localhost:8000/health/readiness").mock(
        return_value=Response(indexing)
    )


@pytest.mark.asyncio
async def test_all_healthy_shows_ok_for_every_service() -> None:
    with respx.mock as mock:
        _mock_all(mock)
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()  # _check_all worker + asyncio.gather complete
            table = pilot.app.screen.query_one("#health-table", DataTable)
            rows = [table.get_row_at(i) for i in range(table.row_count)]

    assert len(rows) == 4
    assert all("OK" in str(row[1]) for row in rows)


@pytest.mark.asyncio
async def test_one_service_down_shows_fail_others_ok() -> None:
    with respx.mock as mock:
        _mock_all(mock, indexing=503)
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            table = pilot.app.screen.query_one("#health-table", DataTable)
            rows = {str(table.get_row_at(i)[0]): table.get_row_at(i) for i in range(table.row_count)}

    assert "FAIL" in str(rows["Indexing"][1])
    assert "OK" in str(rows["Storage"][1])
    assert "OK" in str(rows["Gateway"][1])
    assert "OK" in str(rows["Data Sources"][1])


@pytest.mark.asyncio
async def test_connection_error_shows_fail_with_no_latency() -> None:
    with respx.mock as mock:
        mock.get(f"{_GW}/health/liveness").mock(return_value=Response(200))
        mock.get("http://localhost:9500/health/readiness").mock(return_value=Response(200))
        mock.get("http://localhost:8000/health/readiness").mock(return_value=Response(200))
        mock.get("http://localhost:7000/health/readiness").mock(
            side_effect=ConnectError("refused")
        )
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            table = pilot.app.screen.query_one("#health-table", DataTable)
            rows = {str(table.get_row_at(i)[0]): table.get_row_at(i) for i in range(table.row_count)}

    assert "FAIL" in str(rows["Storage"][1])
    assert str(rows["Storage"][2]) == "—"


@pytest.mark.asyncio
async def test_r_refreshes() -> None:
    with respx.mock as mock:
        _mock_all(mock)
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            table = pilot.app.screen.query_one("#health-table", DataTable)
            before = table.row_count

            await pilot.press("r")
            await pilot.pause()
            await pilot.pause()
            after = table.row_count

    assert before == 4
    assert after == 4


@pytest.mark.asyncio
async def test_b_pops_screen() -> None:
    from src.cli.tui.screens.home import HomeScreen

    class Host(App):
        CSS = ""

        async def on_mount(self) -> None:
            await self.push_screen(HomeScreen(gateway_url=_GW))

    with respx.mock as mock:
        _mock_all(mock)
        mock.get("http://localhost:9500/data-sources").mock(return_value=Response(200, json=[]))
        async with Host().run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            await pilot.app.push_screen(HealthScreen(gateway_url=_GW))
            await pilot.pause()
            await pilot.pause()
            assert isinstance(pilot.app.screen, HealthScreen)

            await pilot.press("b")
            await pilot.pause()
            assert isinstance(pilot.app.screen, HomeScreen)
