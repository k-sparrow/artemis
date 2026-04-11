"""Fixtures for the HTTP sink KafkaConnect container tests.

Minimal stack: Kafka broker + KafkaConnect (artemis image) + WireMock (intake stub).
No volume mounts — messages are produced programmatically by the tests.
"""

from __future__ import annotations

import time
from typing import Iterator

import httpx
import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.network import Network
from wiremock.testing.testcontainer import WireMockContainer

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_FILESYSTEM_INTAKE_TOPIC = "artemis.datasource.filesystem.intake"
_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_WIREMOCK_INTERNAL_PORT = 8080


def _wait_for_http(url: str, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=3.0).status_code < 500:
                return
        except httpx.RequestError:
            pass
        time.sleep(2)
    raise TimeoutError(f"{url} did not become ready within {timeout}s")


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


# ---------------------------------------------------------------------------
# Kafka broker
# ---------------------------------------------------------------------------


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
def kafka_bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_topic(kafka_bootstrap_server: str) -> str:
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(_FILESYSTEM_INTAKE_TOPIC, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()
    return _FILESYSTEM_INTAKE_TOPIC


# ---------------------------------------------------------------------------
# WireMock — stubs the intake endpoint
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def wiremock(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> WireMockContainer:
    wm = WireMockContainer(secure=False)
    wm.with_network(docker_network)
    wm.with_network_aliases("wiremock")
    wm.with_mapping(
        "intake-stub.json",
        {
            "request": {"method": "POST", "url": "/intake"},
            "response": {"status": 202},
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def wiremock_internal_url() -> str:
    """URL that containers on the Docker network use to reach WireMock."""
    return f"http://wiremock:{_WIREMOCK_INTERNAL_PORT}"


@pytest.fixture(scope="session")
def wiremock_host_url(wiremock: WireMockContainer) -> str:
    """URL that the test runner uses to reach WireMock (via exposed port)."""
    host = wiremock.get_container_host_ip()
    port = wiremock.get_exposed_port(_WIREMOCK_INTERNAL_PORT)
    return f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# KafkaConnect (function-scoped — fresh instance per test)
# ---------------------------------------------------------------------------


@pytest.fixture
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    wiremock: WireMockContainer,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(docker_network)
        .with_network_aliases("kafka-connect")
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()


# ---------------------------------------------------------------------------
# Per-test WireMock request log reset
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_wiremock_requests(wiremock_host_url: str) -> Iterator[None]:
    """Clear WireMock request journal before each test."""
    httpx.delete(f"{wiremock_host_url}/__admin/requests")
    yield
