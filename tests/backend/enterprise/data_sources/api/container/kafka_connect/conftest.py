"""Fixtures for KafkaConnect container tests.

Minimal stack: Kafka broker + KafkaConnect (artemis image) only.
No Postgres, WireMock, or data_sources service.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_FILESYSTEM_TOPIC = "artemis.datasource.filesystem"
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
def kafka_bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_topic(kafka_bootstrap_server: str) -> str:
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(_FILESYSTEM_TOPIC, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()
    return _FILESYSTEM_TOPIC


@pytest.fixture
def watch_dir() -> Iterator[Path]:
    """Temporary directory with two pre-seeded files.

    Uses tempfile.mkdtemp() rather than pytest's tmp_path because Bazel's
    linux-sandbox remounts /tmp as a private tmpfs — paths under it are
    invisible to Docker volume mounts, which resolve against the real host
    filesystem. mkdtemp() writes to the real /tmp which Docker can mount.
    """
    import shutil
    import tempfile

    d = Path(tempfile.mkdtemp(prefix="kc-test-watch-"))
    (d / "doc1.txt").write_text("first document")
    (d / "doc2.txt").write_text("second document")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    watch_dir: Path,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(docker_network)
        .with_network_aliases("kafka-connect")
        .with_volume_mapping(str(watch_dir), "/watch", "ro")
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()
