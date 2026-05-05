"""Fixtures for enterprise ksqlDB pipeline contract tests.

Stack: Network → Kafka (KRaft) → KSQLDbContainer → artemis/ksqldb-enterprise-init sidecar

The `streams` fixture creates the two enterprise streams from artemis_enterprise_init.ksql:
  1. artemis-enterprise-datasource-filesystem  (source stream; also creates the backing topic)
  2. artemis-enterprise-datasource-filesystem-intake  (CSAS; routes to HTTP sink)

Prerequisites:
    bazel run //tools/oci/images/ksqldb:enterprise_init_tarball
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka import KSQLDbContainer, SchemaRegistryContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_KAFKA_ALIAS = "broker"
_KSQLDB_ALIAS = "ksqldb-server"
_SR_ALIAS = "schema-registry"
_ENTERPRISE_INIT_IMAGE = "artemis/ksqldb-enterprise-init:latest"


class _NoWait:
    def wait_until_ready(self, container: DockerContainer) -> None:
        pass


@pytest.fixture(scope="session")
def network() -> Generator[Network, None, None]:
    with Network() as n:
        yield n


@pytest.fixture(scope="session")
def kafka(network: Network) -> Generator[KafkaContainer, None, None]:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(network)
        .with_network_aliases(_KAFKA_ALIAS)
        .with_kwargs(hostname=_KAFKA_ALIAS)
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def bootstrap_server(kafka: KafkaContainer) -> str:
    return kafka.get_bootstrap_server()


@pytest.fixture(scope="session")
def schema_registry(
    network: Network, kafka: KafkaContainer
) -> Generator[SchemaRegistryContainer, None, None]:
    container = (
        SchemaRegistryContainer(bootstrap_servers=kafka.get_internal_bootstrap_server())
        .with_network(network)
        .with_network_aliases(_SR_ALIAS)
        .with_log_level("WARN")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def ksqldb(
    network: Network,
    kafka: KafkaContainer,
    schema_registry: SchemaRegistryContainer,
) -> Generator[KSQLDbContainer, None, None]:
    container = (
        KSQLDbContainer(bootstrap_servers=kafka.get_internal_bootstrap_server())
        .with_network(network)
        .with_network_aliases(_KSQLDB_ALIAS)
        .with_schema_registry(schema_registry.get_internal_url())
        .with_log_level("WARN")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def streams(network: Network, ksqldb: KSQLDbContainer) -> None:
    """Run the artemis/ksqldb-enterprise-init sidecar to create the enterprise streams.

    CREATE STREAM IF NOT EXISTS on the source stream auto-creates the backing
    artemis.datasource.filesystem topic, so no pre-existing topic is required.
    """
    internal_url = f"http://{_KSQLDB_ALIAS}:{KSQLDbContainer.HTTP_PORT}"
    container = (
        DockerContainer(_ENTERPRISE_INIT_IMAGE)
        .with_network(network)
        .with_env("KSQLDB_SERVER_URL", internal_url)
        .waiting_for(_NoWait())
    )
    container.start()
    result = container.get_wrapped_container().wait(timeout=120)
    if result["StatusCode"] != 0:
        stdout, stderr = container.get_logs()
        container.stop()
        logs = f"stdout:\n{stdout.decode()}\nstderr:\n{stderr.decode()}"
        raise RuntimeError(
            f"artemis/ksqldb-enterprise-init exited with {result['StatusCode']}\n{logs}"
        )
    container.stop()
