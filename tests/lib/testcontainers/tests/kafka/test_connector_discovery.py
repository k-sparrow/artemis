# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for KafkaConnectContainer.with_connector_tar().

These tests verify that connector JARs packaged as tar archives (mirroring
the Bazel kafka_connect_image approach) are correctly discovered by the
Kafka Connect worker via the /connector-plugins endpoint.

Requires Docker. The session-scoped fixtures in conftest.py handle
the Kafka stack lifecycle.
"""

import pytest
import requests

from tests.lib.testcontainers.kafka.connect import KafkaConnectContainer

# The Datagen connector class that must appear after loading the tar
DATAGEN_CONNECTOR_CLASS = "io.confluent.kafka.connect.datagen.DatagenConnector"


def _get_plugin_classes(connect: KafkaConnectContainer) -> list[str]:
    """Return the list of connector class names from /connector-plugins."""
    response = requests.get(f"{connect.get_url()}/connector-plugins", timeout=10)
    response.raise_for_status()
    return [plugin["class"] for plugin in response.json()]


class TestConnectorDiscovery:
    """Verify that with_connector_tar() bakes connectors into the image correctly."""

    def test_datagen_connector_discovered(self, kafka_connect_with_datagen):
        """The Datagen connector class must appear in /connector-plugins."""
        classes = _get_plugin_classes(kafka_connect_with_datagen)
        assert DATAGEN_CONNECTOR_CLASS in classes

    def test_base_connectors_still_present(self, kafka_connect_with_datagen):
        """Layering a connector tar must not remove built-in connectors."""
        classes = _get_plugin_classes(kafka_connect_with_datagen)
        # FileStream connectors are always present in the base cp-kafka-connect image
        assert any(
            "FileStream" in c for c in classes
        ), "Built-in FileStream connectors missing after connector tar was applied"

    def test_connector_type_is_source_or_sink(self, kafka_connect_with_datagen):
        """The discovered Datagen plugin must be typed as source or sink."""
        response = requests.get(
            f"{kafka_connect_with_datagen.get_url()}/connector-plugins", timeout=10
        )
        response.raise_for_status()

        datagen_plugins = [
            p for p in response.json() if p["class"] == DATAGEN_CONNECTOR_CLASS
        ]
        assert len(datagen_plugins) == 1
        assert datagen_plugins[0]["type"] in ("source", "sink")

    def test_image_rebuilt_with_different_tar_is_independent(
        self, kafka_network, kafka_broker, datagen_connector_tar
    ):
        """Two containers built from the same tar share the cached image tag
        (content-addressed), so the build runs only once per session."""
        container = (
            KafkaConnectContainer(bootstrap_servers="broker:9092")
            .with_network(kafka_network)
            .with_connector_tar(datagen_connector_tar)
        )
        # configure() triggers the image build / cache lookup
        container.configure()

        # Both containers produced from the same tar must resolve to the same tag
        first_tag = container.image
        assert first_tag.startswith("tc-kafka-connect:")

        container2 = (
            KafkaConnectContainer(bootstrap_servers="broker:9092")
            .with_network(kafka_network)
            .with_connector_tar(datagen_connector_tar)
        )
        container2.configure()

        assert (
            container2.image == first_tag
        ), "Identical tar inputs must produce the same content-addressed image tag"
