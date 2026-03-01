# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Configuration tests for KSQLDbContainer.

These tests validate the container's configuration (env vars, ports, image)
without starting any Docker containers.
"""

from testcontainers.core.wait_strategies import HttpWaitStrategy, LogMessageWaitStrategy

from tests.lib.testcontainers.kafka.ksqldb import KSQLDbContainer


BOOTSTRAP = "broker:9092"


class TestKSQLDbContainerDefaults:
    """Verify default configuration after construction."""

    def test_default_image(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.image == KSQLDbContainer.DEFAULT_IMAGE

    def test_http_port_exposed(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert KSQLDbContainer.HTTP_PORT in c.ports

    def test_bootstrap_servers_set(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["KSQL_BOOTSTRAP_SERVERS"] == BOOTSTRAP

    def test_host_name_set(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["KSQL_HOST_NAME"] == "ksqldb-server"

    def test_listeners_set(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["KSQL_LISTENERS"] == (
            f"http://0.0.0.0:{KSQLDbContainer.HTTP_PORT}"
        )

    def test_cache_buffering_disabled(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.env["KSQL_CACHE_MAX_BYTES_BUFFERING"] == "0"

    def test_wait_strategy_is_http(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert isinstance(c._wait_strategy, HttpWaitStrategy)

    def test_custom_image(self):
        custom = "confluentinc/cp-ksqldb-server:7.6.0"
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP, image=custom)
        assert c.image == custom


class TestKSQLDbContainerFluentMethods:
    """Verify fluent configuration methods."""

    def test_with_service_id(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP).with_service_id("my-app")
        assert c.env["KSQL_KSQL_SERVICE_ID"] == "my-app"

    def test_with_service_id_returns_self(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_service_id("x") is c

    def test_with_connect(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP).with_connect(
            "http://connect:8083"
        )
        assert c.env["KSQL_KSQL_CONNECT_URL"] == "http://connect:8083"

    def test_with_connect_returns_self(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_connect("http://connect:8083") is c

    def test_with_schema_registry(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP).with_schema_registry(
            "http://schema-registry:8081"
        )
        assert c.env["KSQL_KSQL_SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"

    def test_with_schema_registry_returns_self(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_schema_registry("http://schema-registry:8081") is c

    def test_with_log_level(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP).with_log_level("DEBUG")
        assert c.env["KSQL_LOG4J_ROOT_LOGLEVEL"] == "DEBUG"

    def test_with_log_level_returns_self(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_log_level("DEBUG") is c

    def test_with_queries_file_sets_env_var(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP).with_queries_file(
            "/queries.sql"
        )
        assert c.env["KSQL_KSQL_QUERIES_FILE"] == "/queries.sql"

    def test_with_queries_file_switches_to_log_wait_strategy(self):
        """Headless mode has no HTTP port — must wait on a log message instead."""
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP).with_queries_file(
            "/queries.sql"
        )
        assert isinstance(c._wait_strategy, LogMessageWaitStrategy)

    def test_with_queries_file_returns_self(self):
        c = KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
        assert c.with_queries_file("/queries.sql") is c

    def test_chaining(self):
        """All fluent methods can be chained in a single expression."""
        c = (
            KSQLDbContainer(bootstrap_servers=BOOTSTRAP)
            .with_service_id("analytics")
            .with_connect("http://connect:8083")
            .with_schema_registry("http://schema-registry:8081")
            .with_log_level("WARN")
        )
        assert c.env["KSQL_KSQL_SERVICE_ID"] == "analytics"
        assert c.env["KSQL_KSQL_CONNECT_URL"] == "http://connect:8083"
        assert c.env["KSQL_KSQL_SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"
        assert c.env["KSQL_LOG4J_ROOT_LOGLEVEL"] == "WARN"
