"""Contract tests for the APISIX init script.

Verifies that the init script calls the APISIX Admin API with the exact
request shape expected — correct endpoints, body structure, and auth header.
The Admin API is stubbed by WireMock; no real APISIX or etcd required.
"""

from __future__ import annotations

import json

import pytest

_ADMIN_KEY = "artemis-apisix-admin-key"


def _find_put(received_requests: list[dict], url: str) -> dict:
    matches = [
        r
        for r in received_requests
        if r["request"]["method"] == "PUT" and r["request"]["url"] == url
    ]
    assert matches, f"No PUT request to {url!r} found in request journal"
    return matches[0]["request"]


def _api_key(request: dict) -> str:
    headers = {k.lower(): v for k, v in request["headers"].items()}
    values = headers["x-api-key"]
    return values["values"][0] if isinstance(values, dict) else values


# ---------------------------------------------------------------------------
# /mcp route registration
# ---------------------------------------------------------------------------


class TestMcpRouteRegistration:
    @pytest.mark.integration
    def test_put_called(self, received_requests: list[dict]) -> None:
        _find_put(received_requests, "/apisix/admin/routes/mcp")

    @pytest.mark.integration
    def test_uri(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/mcp")
        assert json.loads(req["body"])["uri"] == "/mcp/*"

    @pytest.mark.integration
    def test_upstream_type(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/mcp")
        assert json.loads(req["body"])["upstream"]["type"] == "roundrobin"

    @pytest.mark.integration
    def test_upstream_node(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/mcp")
        nodes = json.loads(req["body"])["upstream"]["nodes"]
        assert "backend-mcp:11000" in nodes

    @pytest.mark.integration
    def test_admin_key_header(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/mcp")
        assert _api_key(req) == _ADMIN_KEY


# ---------------------------------------------------------------------------
# /data-sources route registration
# ---------------------------------------------------------------------------


class TestDataSourcesRouteRegistration:
    @pytest.mark.integration
    def test_put_called(self, received_requests: list[dict]) -> None:
        _find_put(received_requests, "/apisix/admin/routes/data-sources")

    @pytest.mark.integration
    def test_uris(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/data-sources")
        assert json.loads(req["body"])["uris"] == ["/data-sources", "/data-sources/*"]

    @pytest.mark.integration
    def test_upstream_type(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/data-sources")
        assert json.loads(req["body"])["upstream"]["type"] == "roundrobin"

    @pytest.mark.integration
    def test_upstream_node(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/data-sources")
        nodes = json.loads(req["body"])["upstream"]["nodes"]
        assert "backend-enterprise-data-sources:9500" in nodes

    @pytest.mark.integration
    def test_admin_key_header(self, received_requests: list[dict]) -> None:
        req = _find_put(received_requests, "/apisix/admin/routes/data-sources")
        assert _api_key(req) == _ADMIN_KEY
