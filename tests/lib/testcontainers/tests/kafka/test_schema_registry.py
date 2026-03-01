# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Configuration tests for SchemaRegistryContainer.

These tests validate the container's configuration (env vars, ports, image)
without starting any Docker containers.
"""

from testcontainers.core.wait_strategies import HttpWaitStrategy

from tests.lib.testcontainers.kafka.schema_registry import SchemaRegistryContainer


BOOTSTRAP = "broker:9092"


class TestSchemaRegistryContainerDefaults:
    """Verify default configuration after construction."""

    def test_default_image(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.image == SchemaRegistryContainer.DEFAULT_IMAGE

    def test_http_port_exposed(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert SchemaRegistryContainer.HTTP_PORT in c.ports

    def test_bootstrap_servers_set(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"] == BOOTSTRAP

    def test_host_name_set(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["SCHEMA_REGISTRY_HOST_NAME"] == "schema-registry"

    def test_listeners_set(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["SCHEMA_REGISTRY_LISTENERS"] == (
            f"http://0.0.0.0:{SchemaRegistryContainer.HTTP_PORT}"
        )

    def test_wait_strategy_is_http(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert isinstance(c._wait_strategy, HttpWaitStrategy)

    def test_custom_image(self):
        custom = "confluentinc/cp-schema-registry:7.6.0"
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP, image=custom)
        assert c.image == custom


class TestSchemaRegistryContainerFluentMethods:
    """Verify fluent configuration methods."""

    def test_with_cluster_id(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP).with_cluster_id(
            "sr-cluster"
        )
        assert c.env["SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID"] == "sr-cluster"

    def test_with_cluster_id_returns_self(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_cluster_id("sr-cluster") is c

    def test_with_schemas_topic(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP).with_schemas_topic(
            "_custom_schemas"
        )
        assert c.env["SCHEMA_REGISTRY_KAFKASTORE_TOPIC"] == "_custom_schemas"

    def test_with_schemas_topic_returns_self(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_schemas_topic("_custom_schemas") is c

    def test_with_log_level(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP).with_log_level("ERROR")
        assert c.env["SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL"] == "ERROR"

    def test_with_log_level_returns_self(self):
        c = SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_log_level("ERROR") is c

    def test_chaining(self):
        """All fluent methods can be chained in a single expression."""
        c = (
            SchemaRegistryContainer(bootstrap_servers=BOOTSTRAP)
            .with_cluster_id("sr-1")
            .with_schemas_topic("_my_schemas")
            .with_log_level("WARN")
        )
        assert c.env["SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID"] == "sr-1"
        assert c.env["SCHEMA_REGISTRY_KAFKASTORE_TOPIC"] == "_my_schemas"
        assert c.env["SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL"] == "WARN"
