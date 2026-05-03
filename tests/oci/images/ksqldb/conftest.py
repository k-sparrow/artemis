"""Shared session fixtures for ksqlDB OCI init-image integration tests.

Stack:
    Network → Kafka (KRaft) → KSQLDbContainer

Both test modules (test_ksqldb_init, test_enterprise_ksqldb_init) share
this stack.  They operate on different Kafka topics so they can run in the
same ksqlDB instance without colliding.

Prerequisites (images must be loaded into Docker daemon before running):
    bazel run //tools/oci/images/ksqldb:artemis_init_tarball
    bazel run //tools/oci/images/ksqldb:enterprise_init_tarball
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka import KSQLDbContainer, SchemaRegistryContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_KAFKA_ALIAS = "broker"
_KSQLDB_ALIAS = "ksqldb-server"
_SR_ALIAS = "schema-registry"


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
def ksqldb_url(ksqldb: KSQLDbContainer) -> str:
    return ksqldb.get_url()


@pytest.fixture(scope="session")
def ksqldb_internal_url() -> str:
    return f"http://{_KSQLDB_ALIAS}:{KSQLDbContainer.HTTP_PORT}"
