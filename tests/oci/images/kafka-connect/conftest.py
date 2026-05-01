"""Fixtures for artemis/cp-kafka-connect image tests.

Minimal stack: Kafka broker + KafkaConnect (artemis image).
No topics, no sinks — plugin discovery only.
"""

from __future__ import annotations

import pytest
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_KC_IMAGE = "artemis/cp-kafka-connect:latest"


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


@pytest.fixture(scope="session")
def kafka_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> KafkaContainer:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(docker_network)
        .with_network_aliases("broker")
        .with_kwargs(hostname="broker")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(docker_network)
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()
