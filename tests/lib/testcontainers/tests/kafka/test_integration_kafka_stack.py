# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for the Kafka Connect and KSQLDb testcontainers.

These tests start real Docker containers and verify that:
  - Kafka Connect starts successfully and joins the Kafka network
  - KSQLDb starts successfully and joins the Kafka network
  - Both services expose healthy REST APIs

All fixtures are session-scoped (see conftest.py), so the Kafka stack is
started once and shared across all tests in this file.

Run with:
    pytest tests/lib/testcontainers/tests/kafka/test_integration_kafka_stack.py -v
"""

import pytest
import requests


# ---------------------------------------------------------------------------
# Kafka Connect
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestKafkaConnectIntegration:
    """Verify that KafkaConnectContainer starts correctly on the Kafka network."""

    def test_connectors_endpoint_is_reachable(self, kafka_connect):
        """GET /connectors must return HTTP 200."""
        url = kafka_connect.get_url()
        response = requests.get(url + "/connectors", timeout=10)
        assert response.status_code == 200

    def test_connectors_list_is_empty_on_fresh_worker(self, kafka_connect):
        """A newly-started Connect worker has no connectors deployed."""
        url = kafka_connect.get_url()
        connectors = requests.get(url + "/connectors", timeout=10).json()
        assert connectors == []

    def test_connector_plugins_endpoint_is_reachable(self, kafka_connect):
        """GET /connector-plugins must return HTTP 200."""
        url = kafka_connect.get_url()
        response = requests.get(url + "/connector-plugins", timeout=10)
        assert response.status_code == 200

    def test_connector_plugins_list_is_non_empty(self, kafka_connect):
        """The base image ships with built-in connector plugins."""
        url = kafka_connect.get_url()
        plugins = requests.get(url + "/connector-plugins", timeout=10).json()
        assert len(plugins) > 0

    def test_each_plugin_has_required_fields(self, kafka_connect):
        """Every plugin entry must have 'class', 'type', and 'version' fields."""
        url = kafka_connect.get_url()
        plugins = requests.get(url + "/connector-plugins", timeout=10).json()
        for plugin in plugins:
            assert "class" in plugin, f"Plugin missing 'class': {plugin}"
            assert "type" in plugin, f"Plugin missing 'type': {plugin}"
            assert "version" in plugin, f"Plugin missing 'version': {plugin}"


# ---------------------------------------------------------------------------
# KSQLDb
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestKSQLDbIntegration:
    """Verify that KSQLDbContainer starts correctly on the Kafka network."""

    def test_healthcheck_endpoint_is_reachable(self, ksqldb_server):
        """GET /healthcheck must return HTTP 200."""
        url = ksqldb_server.get_url()
        response = requests.get(url + "/healthcheck", timeout=10)
        assert response.status_code == 200

    def test_healthcheck_reports_healthy(self, ksqldb_server):
        """GET /healthcheck must report isHealthy: true."""
        url = ksqldb_server.get_url()
        body = requests.get(url + "/healthcheck", timeout=10).json()
        assert body.get("isHealthy") is True

    def test_info_endpoint_is_reachable(self, ksqldb_server):
        """GET /info must return HTTP 200."""
        url = ksqldb_server.get_url()
        response = requests.get(url + "/info", timeout=10)
        assert response.status_code == 200

    def test_info_contains_server_info_key(self, ksqldb_server):
        """GET /info response must contain the 'KsqlServerInfo' object."""
        url = ksqldb_server.get_url()
        body = requests.get(url + "/info", timeout=10).json()
        assert "KsqlServerInfo" in body

    def test_info_reports_kafka_cluster_id(self, ksqldb_server):
        """KsqlServerInfo must include a non-empty kafkaClusterId once connected."""
        url = ksqldb_server.get_url()
        info = requests.get(url + "/info", timeout=10).json()["KsqlServerInfo"]
        assert info.get("kafkaClusterId"), "kafkaClusterId should be non-empty"

    def test_info_reports_server_status(self, ksqldb_server):
        """KsqlServerInfo must include a non-empty serverStatus field."""
        url = ksqldb_server.get_url()
        info = requests.get(url + "/info", timeout=10).json()["KsqlServerInfo"]
        assert info.get("serverStatus"), "serverStatus should be non-empty"
