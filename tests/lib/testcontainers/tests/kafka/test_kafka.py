# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Configuration tests for KafkaContainer.

These tests validate the wrapper's behaviour without starting any Docker
containers.  All assertions run against the container object's in-memory
state (aliases list, constant values), so they are fast and require no
Docker daemon.
"""

import pytest
from testcontainers.kafka import KafkaContainer as UpstreamKafkaContainer

from tests.lib.testcontainers.kafka.kafka import KafkaContainer


class TestKafkaContainerSubclass:
    """Verify inheritance and class-level constants."""

    def test_is_subclass_of_upstream(self):
        assert issubclass(KafkaContainer, UpstreamKafkaContainer)

    def test_internal_port_constant_is_broker_port(self):
        assert KafkaContainer.KAFKA_INTERNAL_PORT == 9092


class TestGetInternalBootstrapServer:
    """Verify get_internal_bootstrap_server() behaviour."""

    def test_raises_when_no_alias_set(self):
        container = KafkaContainer()
        with pytest.raises(RuntimeError, match="No network alias"):
            container.get_internal_bootstrap_server()

    def test_returns_alias_colon_internal_port(self):
        container = KafkaContainer().with_network_aliases("broker")
        assert container.get_internal_bootstrap_server() == "broker:9092"

    def test_uses_first_alias_when_multiple_passed_in_one_call(self):
        # Pass multiple aliases in a single with_network_aliases() call.
        # Chaining two separate calls replaces rather than accumulates —
        # this is upstream DockerContainer behaviour.
        container = KafkaContainer().with_network_aliases("broker", "kafka")
        assert container.get_internal_bootstrap_server() == "broker:9092"

    def test_custom_alias_name(self):
        container = KafkaContainer().with_network_aliases("itzik")
        assert container.get_internal_bootstrap_server() == "itzik:9092"

    def test_error_message_mentions_with_network_aliases(self):
        container = KafkaContainer()
        with pytest.raises(RuntimeError, match="with_network_aliases"):
            container.get_internal_bootstrap_server()

    def test_result_usable_as_bootstrap_servers_string(self):
        """Returned string must be host:port — no scheme, no extra whitespace."""
        result = (
            KafkaContainer()
            .with_network_aliases("broker")
            .get_internal_bootstrap_server()
        )
        host, _, port = result.rpartition(":")
        assert host == "broker"
        assert port.isdigit()
