# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Kafka ecosystem testcontainers.

Provides testcontainer implementations for Confluent Platform components
that are not available in the upstream testcontainers-python library.

Supported containers:
  - KafkaContainer: upstream wrapper with get_internal_bootstrap_server()
  - KafkaConnectContainer: Kafka Connect REST API server
  - KSQLDbContainer: ksqlDB server
  - SchemaRegistryContainer: Confluent Schema Registry

Network setup:

  Kafka Connect, ksqlDB, and Schema Registry must reach the Kafka broker
  via Docker-internal networking.  Use our KafkaContainer wrapper so you
  can resolve the internal address without hardcoding it:

      from testcontainers.core.network import Network
      from tests.lib.testcontainers.kafka import (
          KafkaContainer,
          KafkaConnectContainer,
      )

      with Network() as network:
          kafka = (
              KafkaContainer()
              .with_kraft()
              .with_network(network)
              .with_network_aliases("broker")
          )
          kafka.start()

          connect = KafkaConnectContainer(
              bootstrap_servers=kafka.get_internal_bootstrap_server(),
          ).with_network(network)
          connect.start()
"""

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer
from tests.lib.testcontainers.kafka.ksqldb import KSQLDbContainer
from tests.lib.testcontainers.kafka.schema_registry import SchemaRegistryContainer

__all__ = [
    "KafkaContainer",
    "KafkaConnectContainer",
    "KSQLDbContainer",
    "SchemaRegistryContainer",
]
