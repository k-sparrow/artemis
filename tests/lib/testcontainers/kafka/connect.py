# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
# Converted from the Java cp-testcontainers library:
# https://github.com/testcontainers-all-things-kafka/cp-testcontainers
#
"""Kafka Connect testcontainer."""

from __future__ import annotations

import hashlib
import shutil
import tempfile
from pathlib import Path
from typing import List

from typing_extensions import Self
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy


__all__ = ["KafkaConnectContainer"]

# Port Kafka uses for its BROKER (internal) listener on a Docker network.
# When containers share a network, this is the port Connect must target.
KAFKA_BROKER_INTERNAL_PORT = 9092


class KafkaConnectContainer(DockerContainer):
    """Testcontainer for Kafka Connect (distributed mode).

    Kafka Connect exposes a REST API on port 8083. This container requires
    a reachable Kafka broker, which in integration tests is typically another
    container on the same Docker network.

    Args:
        bootstrap_servers: Kafka bootstrap address reachable from within Docker
            (e.g. ``"broker:9092"`` when Kafka has the network alias ``"broker"``).
        image: Confluent Platform Kafka Connect Docker image.
        cluster_id: Connect worker group ID, also used as the prefix for the
            three internal Kafka topics (configs, offsets, status).

    Example::

        from testcontainers.core.network import Network
        from testcontainers.kafka import KafkaContainer
        from tests.lib.testcontainers.kafka import KafkaConnectContainer

        with Network() as network:
            kafka = (
                KafkaContainer()
                .with_kraft()
                .with_network(network)
                .with_network_aliases("broker")
            )
            kafka.start()

            connect = (
                KafkaConnectContainer(bootstrap_servers="broker:9092")
                .with_network(network)
            )
            connect.start()

            url = connect.get_url()   # http://localhost:<mapped-port>
    """

    HTTP_PORT = 8083
    DEFAULT_IMAGE = "confluentinc/cp-kafka-connect:7.8.0"

    def __init__(
        self,
        bootstrap_servers: str,
        image: str = DEFAULT_IMAGE,
        cluster_id: str = "connect",
    ) -> None:
        super().__init__(image)
        self._cluster_id = cluster_id
        self._connector_tars: List[Path] = []

        self.with_exposed_ports(self.HTTP_PORT)

        # Bootstrap
        self.with_env("CONNECT_BOOTSTRAP_SERVERS", bootstrap_servers)

        # Worker identity and internal coordination topics.
        # Replication factor 1 is correct for single-broker test clusters.
        self.with_env("CONNECT_GROUP_ID", cluster_id)
        self.with_env("CONNECT_CONFIG_STORAGE_TOPIC", f"{cluster_id}-configs")
        self.with_env("CONNECT_OFFSET_STORAGE_TOPIC", f"{cluster_id}-offsets")
        self.with_env("CONNECT_STATUS_STORAGE_TOPIC", f"{cluster_id}-status")
        self.with_env("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
        self.with_env("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
        self.with_env("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
        self.with_env("CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR", "1")

        # Converters: JSON is the most portable default for test environments
        self.with_env(
            "CONNECT_KEY_CONVERTER",
            "org.apache.kafka.connect.json.JsonConverter",
        )
        self.with_env(
            "CONNECT_VALUE_CONVERTER",
            "org.apache.kafka.connect.json.JsonConverter",
        )

        # REST listener configuration
        self.with_env("CONNECT_REST_PORT", str(self.HTTP_PORT))
        self.with_env("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")
        self.with_env("CONNECT_LISTENERS", f"http://0.0.0.0:{self.HTTP_PORT}")

        # Allow connectors to override client configs (needed for Kafka HTTP Sink)
        self.with_env("CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY", "All")

        # Plugin discovery path — include confluent-hub-components for Camel connectors
        self.with_env(
            "CONNECT_PLUGIN_PATH",
            "/usr/share/java,/usr/share/confluent-hub-components",
        )

        # JVM heap — keep modest for test environments
        self.with_env("KAFKA_HEAP_OPTS", "-Xmx1G -Xms512M")

        # Block until the REST API reports workers are up
        self.waiting_for(
            HttpWaitStrategy(self.HTTP_PORT, "/connectors").for_status_code(200)
        )

    def with_connector_tar(self, connector_tar: Path) -> Self:
        """Layer connector JARs from a tar archive into the container image.

        Mirrors the Bazel ``kafka_connect_image`` approach: the tar is applied
        as a Docker layer on top of the base image via ``ADD <tar> /``.

        The tar must use **absolute-ish paths** — paths relative to the
        container filesystem root, without a leading ``/``.  For example, a
        JAR that should land at ``/usr/share/java/my-connector/my.jar`` must
        be archived as ``usr/share/java/my-connector/my.jar``.  This is the
        exact layout that Bazel's ``mtree_mutate`` + ``tar`` rules produce.

        Multiple calls accumulate tars; each becomes a separate image layer.
        The custom image is built lazily just before the container starts.

        Args:
            connector_tar: Path to the tar archive on the host.
        """
        self._connector_tars.append(Path(connector_tar))
        return self

    def configure(self) -> None:
        """Build a custom image when connector tars have been registered."""
        if self._connector_tars:
            self.image = self._build_layered_image()

    def _build_layered_image(self) -> str:
        """Build and cache a custom Docker image with connector tars as layers.

        Uses a content-based tag (SHA-256 of all tar bytes) so the image is
        only rebuilt when its inputs change.
        """
        import docker as docker_sdk

        # Derive a stable, content-based image tag
        hasher = hashlib.sha256()
        for tar_path in self._connector_tars:
            hasher.update(tar_path.read_bytes())
        tag = f"tc-kafka-connect:{hasher.hexdigest()[:16]}"

        client = docker_sdk.from_env()

        try:
            client.images.get(tag)
            return tag  # already built — reuse
        except docker_sdk.errors.ImageNotFound:
            pass

        build_ctx = tempfile.mkdtemp(prefix="tc-kafka-connect-build-")
        try:
            ctx = Path(build_ctx)

            # Copy tars into the build context and emit one ADD per tar.
            # Docker's ADD extracts tars relative to the destination (/), so
            # a path like usr/share/java/my-connector/my.jar inside the tar
            # lands at /usr/share/java/my-connector/my.jar — exactly as
            # Bazel's oci_image does with tar layers.
            add_lines: List[str] = []
            for i, tar_path in enumerate(self._connector_tars):
                name = f"connector-{i}.tar"
                shutil.copy2(tar_path, ctx / name)
                add_lines.append(f"ADD {name} /")

            dockerfile = f"FROM {self.image}\n" + "\n".join(add_lines) + "\n"
            (ctx / "Dockerfile").write_text(dockerfile)

            client.images.build(path=build_ctx, tag=tag, rm=True)
        finally:
            shutil.rmtree(build_ctx, ignore_errors=True)

        return tag

    def with_cluster_id(self, cluster_id: str) -> Self:
        """Override the Connect worker group ID and internal topic names."""
        self._cluster_id = cluster_id
        self.with_env("CONNECT_GROUP_ID", cluster_id)
        self.with_env("CONNECT_CONFIG_STORAGE_TOPIC", f"{cluster_id}-configs")
        self.with_env("CONNECT_OFFSET_STORAGE_TOPIC", f"{cluster_id}-offsets")
        self.with_env("CONNECT_STATUS_STORAGE_TOPIC", f"{cluster_id}-status")
        return self

    def with_replication_factor(self, rf: int) -> Self:
        """Set all replication factors (use 1 for single-broker test clusters)."""
        for key in (
            "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR",
            "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR",
            "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR",
            "CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR",
        ):
            self.with_env(key, str(rf))
        return self

    def with_log_level(self, level: str) -> Self:
        """Set the root log level (e.g. ``"WARN"``, ``"DEBUG"``)."""
        self.with_env("CONNECT_LOG4J_ROOT_LOGLEVEL", level)
        return self

    def get_url(self) -> str:
        """Return the Connect REST API base URL reachable from the test process."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.HTTP_PORT)
        return f"http://{host}:{port}"

    def get_internal_url(self) -> str:
        """Return the Connect REST API URL for other containers on the same network.

        Uses the first network alias if one has been set, otherwise falls back to
        the container's short ID as the hostname.
        """
        alias = (
            self._network_aliases[0]
            if self._network_aliases
            else self._container.short_id  # type: ignore[union-attr]
        )
        return f"http://{alias}:{self.HTTP_PORT}"
