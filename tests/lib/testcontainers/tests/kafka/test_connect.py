# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Configuration tests for KafkaConnectContainer.

These tests validate the container's configuration (env vars, ports, image)
without starting any Docker containers.
"""

import pytest  # noqa: F401
from testcontainers.core.wait_strategies import HttpWaitStrategy

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer


BOOTSTRAP = "broker:9092"


class TestKafkaConnectContainerDefaults:
    """Verify default configuration after construction."""

    def test_default_image(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.image == KafkaConnectContainer.DEFAULT_IMAGE

    def test_http_port_exposed(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert KafkaConnectContainer.HTTP_PORT in c.ports

    def test_bootstrap_servers_set(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_BOOTSTRAP_SERVERS"] == BOOTSTRAP

    def test_default_group_id(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_GROUP_ID"] == "connect"

    def test_default_internal_topics(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_CONFIG_STORAGE_TOPIC"] == "connect-configs"
        assert c.env["CONNECT_OFFSET_STORAGE_TOPIC"] == "connect-offsets"
        assert c.env["CONNECT_STATUS_STORAGE_TOPIC"] == "connect-status"

    def test_default_replication_factors_are_one(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR"] == "1"
        assert c.env["CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR"] == "1"
        assert c.env["CONNECT_STATUS_STORAGE_REPLICATION_FACTOR"] == "1"
        assert c.env["CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR"] == "1"

    def test_default_converters(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_KEY_CONVERTER"] == (
            "org.apache.kafka.connect.json.JsonConverter"
        )
        assert c.env["CONNECT_VALUE_CONVERTER"] == (
            "org.apache.kafka.connect.json.JsonConverter"
        )

    def test_rest_listener_configured(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_REST_PORT"] == str(KafkaConnectContainer.HTTP_PORT)
        assert c.env["CONNECT_REST_ADVERTISED_HOST_NAME"] == "localhost"
        assert c.env["CONNECT_LISTENERS"] == (
            f"http://0.0.0.0:{KafkaConnectContainer.HTTP_PORT}"
        )

    def test_plugin_path_set(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert (
            c.env["CONNECT_PLUGIN_PATH"]
            == "/usr/share/java,/usr/share/confluent-hub-components"
        )

    def test_client_config_override_policy(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY"] == "All"

    def test_wait_strategy_is_http(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        assert isinstance(c._wait_strategy, HttpWaitStrategy)

    def test_custom_image(self):
        custom = "confluentinc/cp-kafka-connect:7.6.0"
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP, image=custom)
        assert c.image == custom

    def test_custom_cluster_id_at_construction(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP, cluster_id="my-cluster")
        assert c.env["CONNECT_GROUP_ID"] == "my-cluster"
        assert c.env["CONNECT_CONFIG_STORAGE_TOPIC"] == "my-cluster-configs"
        assert c.env["CONNECT_OFFSET_STORAGE_TOPIC"] == "my-cluster-offsets"
        assert c.env["CONNECT_STATUS_STORAGE_TOPIC"] == "my-cluster-status"


class TestKafkaConnectContainerFluentMethods:
    """Verify fluent configuration methods."""

    def test_with_cluster_id_updates_group_id(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP).with_cluster_id("team-a")
        assert c.env["CONNECT_GROUP_ID"] == "team-a"

    def test_with_cluster_id_updates_all_three_topics(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP).with_cluster_id("team-a")
        assert c.env["CONNECT_CONFIG_STORAGE_TOPIC"] == "team-a-configs"
        assert c.env["CONNECT_OFFSET_STORAGE_TOPIC"] == "team-a-offsets"
        assert c.env["CONNECT_STATUS_STORAGE_TOPIC"] == "team-a-status"

    def test_with_cluster_id_returns_self(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        result = c.with_cluster_id("x")
        assert result is c

    def test_with_replication_factor_updates_all_four_keys(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP).with_replication_factor(
            3
        )
        assert c.env["CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR"] == "3"
        assert c.env["CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR"] == "3"
        assert c.env["CONNECT_STATUS_STORAGE_REPLICATION_FACTOR"] == "3"
        assert c.env["CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR"] == "3"

    def test_with_replication_factor_returns_self(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        result = c.with_replication_factor(3)
        assert result is c

    def test_with_log_level(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP).with_log_level("WARN")
        assert c.env["CONNECT_LOG4J_ROOT_LOGLEVEL"] == "WARN"

    def test_with_log_level_returns_self(self):
        c = KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
        result = c.with_log_level("DEBUG")
        assert result is c

    def test_chaining(self):
        """All fluent methods can be chained in a single expression."""
        c = (
            KafkaConnectContainer(bootstrap_servers=BOOTSTRAP)
            .with_cluster_id("chain-test")
            .with_replication_factor(2)
            .with_log_level("ERROR")
        )
        assert c.env["CONNECT_GROUP_ID"] == "chain-test"
        assert c.env["CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR"] == "2"
        assert c.env["CONNECT_LOG4J_ROOT_LOGLEVEL"] == "ERROR"
