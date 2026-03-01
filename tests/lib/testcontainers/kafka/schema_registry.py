# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
# Converted from the Java cp-testcontainers library:
# https://github.com/testcontainers-all-things-kafka/cp-testcontainers
#
"""Confluent Schema Registry testcontainer."""

from __future__ import annotations

from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy


__all__ = ["SchemaRegistryContainer"]


class SchemaRegistryContainer(DockerContainer):
    """Testcontainer for Confluent Schema Registry.

    Schema Registry exposes an HTTP API on port 8081 and requires a
    reachable Kafka broker for its internal ``_schemas`` topic.

    Args:
        bootstrap_servers: Kafka bootstrap address reachable from within Docker
            (e.g. ``"broker:9092"`` when Kafka has the network alias ``"broker"``).
        image: Confluent Platform Schema Registry Docker image.

    Example::

        from testcontainers.core.network import Network
        from testcontainers.kafka import KafkaContainer
        from tests.lib.testcontainers.kafka import SchemaRegistryContainer

        with Network() as network:
            kafka = (
                KafkaContainer()
                .with_kraft()
                .with_network(network)
                .with_network_aliases("broker")
            )
            kafka.start()

            schema_registry = (
                SchemaRegistryContainer(bootstrap_servers="broker:9092")
                .with_network(network)
                .with_network_aliases("schema-registry")
            )
            schema_registry.start()

            url = schema_registry.get_url()   # http://localhost:<mapped-port>

    Passing Schema Registry to ksqlDB::

        ksqldb = (
            KSQLDbContainer(bootstrap_servers="broker:9092")
            .with_network(network)
            .with_schema_registry(schema_registry.get_internal_url())
        )
    """

    HTTP_PORT = 8081
    DEFAULT_IMAGE = "confluentinc/cp-schema-registry:7.8.0"

    def __init__(
        self,
        bootstrap_servers: str,
        image: str = DEFAULT_IMAGE,
    ) -> None:
        super().__init__(image)

        self.with_exposed_ports(self.HTTP_PORT)

        # Server identity
        self.with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")

        # Bootstrap connection to Kafka (kafkastore is the backing Kafka topic store)
        self.with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrap_servers)

        # REST listener
        self.with_env("SCHEMA_REGISTRY_LISTENERS", f"http://0.0.0.0:{self.HTTP_PORT}")

        # Block until Schema Registry reports it can serve subjects
        self.waiting_for(
            HttpWaitStrategy(self.HTTP_PORT, "/subjects").for_status_code(200)
        )

    def with_cluster_id(self, cluster_id: str) -> Self:
        """Set the Schema Registry consumer group ID.

        Useful when running multiple Schema Registry instances against the
        same Kafka cluster (each needs a distinct group ID).
        """
        self.with_env("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", cluster_id)
        return self

    def with_schemas_topic(self, topic: str) -> Self:
        """Override the name of the internal Kafka topic used to store schemas.

        Defaults to ``_schemas``.
        """
        self.with_env("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", topic)
        return self

    def with_log_level(self, level: str) -> Self:
        """Set the root log level (e.g. ``"WARN"``, ``"DEBUG"``)."""
        self.with_env("SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL", level)
        return self

    def get_url(self) -> str:
        """Return the Schema Registry URL reachable from the test process."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"

    def get_internal_url(self) -> str:
        """Return the Schema Registry URL for other containers on the same network.

        Uses the first network alias if one has been set, otherwise falls back to
        the container's short ID as the hostname.
        """
        alias = (
            self._network_aliases[0]
            if self._network_aliases
            else self._container.short_id  # type: ignore[union-attr]
        )
        return f"http://{alias}:{self.HTTP_PORT}"
