# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
# Converted from the Java cp-testcontainers library:
# https://github.com/testcontainers-all-things-kafka/cp-testcontainers
#
"""ksqlDB testcontainer."""

from __future__ import annotations

from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy, LogMessageWaitStrategy


__all__ = ["KSQLDbContainer"]


class KSQLDbContainer(DockerContainer):
    """Testcontainer for ksqlDB server.

    ksqlDB exposes an HTTP API on port 8088.  The server requires a reachable
    Kafka broker, which in integration tests is typically a ``KafkaContainer``
    on the same Docker network.

    Args:
        bootstrap_servers: Kafka bootstrap address reachable from within Docker
            (e.g. ``"broker:9092"``).
        image: Confluent Platform ksqlDB Docker image.

    Example::

        from testcontainers.core.network import Network
        from testcontainers.kafka import KafkaContainer
        from tests.lib.testcontainers.kafka import KSQLDbContainer

        with Network() as network:
            kafka = (
                KafkaContainer()
                .with_kraft()
                .with_network(network)
                .with_network_aliases("broker")
            )
            kafka.start()

            ksqldb = (
                KSQLDbContainer(bootstrap_servers="broker:9092")
                .with_network(network)
            )
            ksqldb.start()

            url = ksqldb.get_url()   # http://localhost:<mapped-port>

    Connecting ksqlDB to Kafka Connect::

        ksqldb = (
            KSQLDbContainer(bootstrap_servers="broker:9092")
            .with_network(network)
            .with_connect(connect.get_internal_url())
        )
    """

    HTTP_PORT = 8088
    DEFAULT_IMAGE = "confluentinc/cp-ksqldb-server:7.8.0"

    def __init__(
        self,
        bootstrap_servers: str,
        image: str = DEFAULT_IMAGE,
    ) -> None:
        super().__init__(image)

        self.with_exposed_ports(self.HTTP_PORT)

        # Bootstrap connection to Kafka
        self.with_env("KSQL_BOOTSTRAP_SERVERS", bootstrap_servers)

        # Server identity
        self.with_env("KSQL_HOST_NAME", "ksqldb-server")
        self.with_env("KSQL_LISTENERS", f"http://0.0.0.0:{self.HTTP_PORT}")

        # Disable in-memory row buffering — simplifies test teardown
        self.with_env("KSQL_CACHE_MAX_BYTES_BUFFERING", "0")

        # Block until ksqlDB reports it is ready
        self.waiting_for(HttpWaitStrategy(self.HTTP_PORT, "/").for_status_code(200))

    def with_service_id(self, service_id: str) -> Self:
        """Set the ksqlDB service ID (used to namespace internal topics).

        Defaults to ``"default_"`` when not set.
        """
        self.with_env("KSQL_KSQL_SERVICE_ID", service_id)
        return self

    def with_connect(self, connect_url: str) -> Self:
        """Integrate ksqlDB with a running Kafka Connect cluster.

        Args:
            connect_url: Internal Connect REST URL reachable from within Docker,
                e.g. ``connect_container.get_internal_url()``.
        """
        self.with_env("KSQL_KSQL_CONNECT_URL", connect_url)
        return self

    def with_schema_registry(self, schema_registry_url: str) -> Self:
        """Configure ksqlDB to use a Schema Registry instance.

        Args:
            schema_registry_url: Internal Schema Registry URL reachable within Docker.
        """
        self.with_env("KSQL_KSQL_SCHEMA_REGISTRY_URL", schema_registry_url)
        return self

    def with_queries_file(self, queries_file_path: str) -> Self:
        """Run ksqlDB in headless mode, executing queries from a file at startup.

        In headless mode the HTTP port is not used; this method switches the
        waiting strategy to a log-message check instead.

        Args:
            queries_file_path: Path **inside the container** to the ``.sql`` file.
        """
        self.with_env("KSQL_KSQL_QUERIES_FILE", queries_file_path)
        # Headless mode: no HTTP port — wait for the ready log line instead
        self.waiting_for(LogMessageWaitStrategy(r".*INFO Server up and running.*"))
        return self

    def with_log_level(self, level: str) -> Self:
        """Set the root log level (e.g. ``"WARN"``, ``"DEBUG"``)."""
        self.with_env("KSQL_LOG4J_ROOT_LOGLEVEL", level)
        return self

    def get_url(self) -> str:
        """Return the ksqlDB REST API base URL reachable from the test process."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"
