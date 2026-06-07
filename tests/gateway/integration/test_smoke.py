"""Smoke tests for the APISIX gateway.

Verifies route registration via the Admin API, that registered paths are
forwarded (non-404), and that all unregistered paths are blocked (404).
"""

from __future__ import annotations

import httpx
import pytest

_ADMIN_KEY = "artemis-apisix-admin-key"


# ---------------------------------------------------------------------------
# Route registration — Admin API confirms routes exist
# ---------------------------------------------------------------------------


class TestRouteRegistration:
    @pytest.mark.integration
    def test_mcp_route_registered(self, admin_url: str) -> None:
        resp = httpx.get(
            f"{admin_url}/apisix/admin/routes/mcp",
            headers={"X-API-KEY": _ADMIN_KEY},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["value"]["uri"] == "/mcp/*"

    @pytest.mark.integration
    def test_data_sources_route_registered(self, admin_url: str) -> None:
        resp = httpx.get(
            f"{admin_url}/apisix/admin/routes/data-sources",
            headers={"X-API-KEY": _ADMIN_KEY},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["value"]["uris"] == ["/data-sources", "/data-sources/*"]


# ---------------------------------------------------------------------------
# Proxy behaviour — registered paths are forwarded (not blocked)
# APISIX returns 502 when the upstream is unreachable, not 404.
# A non-404 response proves the route exists and APISIX attempted to proxy.
# ---------------------------------------------------------------------------


_REGISTERED_PATHS = [
    pytest.param("/mcp/anything", id="mcp"),
    pytest.param("/data-sources/anything", id="data-sources"),
]


class TestRegisteredPathsAreRouted:
    @pytest.mark.integration
    @pytest.mark.parametrize("path", _REGISTERED_PATHS)
    def test_path_is_routed(self, proxy_url: str, path: str) -> None:
        resp = httpx.get(f"{proxy_url}{path}", timeout=5.0)
        assert resp.status_code != 404, (
            f"Expected non-404 for registered path {path!r} "
            f"(upstream unreachable → 502), got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Deny by default — unregistered paths are blocked with 404
# ---------------------------------------------------------------------------


_BLOCKED_PATHS = [
    pytest.param("/", id="root"),
    pytest.param("/namespaces/some-id", id="namespaces"),
    pytest.param("/retrieve/invoke", id="retrieve"),
    pytest.param("/intake", id="intake"),
    pytest.param("/does/not/exist", id="arbitrary"),
]


class TestUnregisteredPathsAreBlocked:
    @pytest.mark.integration
    @pytest.mark.parametrize("path", _BLOCKED_PATHS)
    def test_path_is_blocked(self, proxy_url: str, path: str) -> None:
        resp = httpx.get(f"{proxy_url}{path}", timeout=5.0)
        assert resp.status_code == 404
