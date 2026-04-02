# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Configuration tests for KafkaContainer.

These tests validate the wrapper's behaviour without starting any Docker
containers.  All assertions run against the container object's in-memory
state (aliases list, constant values), so they are fast and require no
Docker daemon.

TestKafkaProduceConsume is an integration test that starts a real container
and verifies basic produce → consume works end-to-end.
"""

from __future__ import annotations

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
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


# ---------------------------------------------------------------------------
# Integration: real container — produce → consume
# ---------------------------------------------------------------------------


class TestKafkaProduceConsume:
    """Start a real KafkaContainer and verify produce → consume works.

    This is the baseline sanity check. If this fails the KafkaContainer
    configuration itself is broken. If this passes but MinIO→Kafka fails,
    the issue is in how MinIO connects to the broker.
    """

    _TOPIC = "test.produce.consume"

    def test_host_producer_host_consumer(self) -> None:
        container = KafkaContainer().with_kraft().with_network_aliases("broker")
        container.start()
        try:
            bootstrap = container.get_bootstrap_server()

            payload = json.dumps({"id": str(uuid.uuid4())}).encode()
            producer = KafkaProducer(bootstrap_servers=[bootstrap])
            producer.send(self._TOPIC, payload)
            producer.flush()
            producer.close()

            consumer = KafkaConsumer(
                self._TOPIC,
                bootstrap_servers=[bootstrap],
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=10_000,
                group_id=None,
            )

            messages = list(consumer)
            consumer.close()

            assert len(messages) == 1
            assert json.loads(messages[0].value)["id"]
        finally:
            container.stop()

    def test_internal_produce_consume_via_kcat(self) -> None:
        """Produce and consume via the internal broker address using kcat containers.

        Both producer and consumer run as Docker containers on the same network,
        using broker:9092 (the BROKER listener). This verifies that the internal
        advertised address is correct and reachable from other containers.
        """
        _TOPIC = "test.kcat.internal"
        _MESSAGE = "hello-from-kcat"

        network = Network()
        network.create()
        try:
            kafka = (
                KafkaContainer()
                .with_kraft()
                .with_network(network)
                .with_network_aliases("broker")
            )
            kafka.start()
            internal_bootstrap = kafka.get_internal_bootstrap_server()

            # Pre-create the topic from the host so kcat doesn't race against
            # async auto-creation and fail with "Unknown topic or partition".
            admin = KafkaAdminClient(bootstrap_servers=[kafka.get_bootstrap_server()])
            admin.create_topics(
                [NewTopic(_TOPIC, num_partitions=1, replication_factor=1)]
            )
            admin.close()

            # Start a long-running kcat container on the network, then exec
            # produce/consume commands into it — same pattern as docker-compose.
            kcat = (
                DockerContainer("confluentinc/cp-kafkacat:7.1.16")
                .with_network(network)
                .with_command("sleep infinity")
            )
            kcat.start()
            wrapped = kcat.get_wrapped_container()

            # Produce one message.
            exit_code, output = wrapped.exec_run(
                f'bash -c \'echo "{_MESSAGE}" | '
                f"kafkacat -b {internal_bootstrap} -t {_TOPIC} -P'"
            )
            assert exit_code == 0, f"produce failed: {output.decode()}"

            # Consume: read from beginning, exit after 1 message.
            exit_code, output = wrapped.exec_run(
                f"kafkacat -b {internal_bootstrap} -t {_TOPIC} -C -e -o beginning -c 1"
            )
            kcat.stop()

            assert _MESSAGE in output.decode(), f"unexpected output: {output.decode()}"
        finally:
            kafka.stop()
