# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""KafkaContainer wrapper with Docker-network-aware bootstrap resolution."""

from __future__ import annotations

from testcontainers.kafka import KafkaContainer as _UpstreamKafkaContainer


__all__ = ["KafkaContainer"]


class KafkaContainer(_UpstreamKafkaContainer):
    """Thin wrapper around the upstream KafkaContainer that adds
    ``get_internal_bootstrap_server()``.

    The upstream ``get_bootstrap_server()`` always returns the host-mapped
    address (``localhost:<random-port>``), which is unreachable from other
    containers on the same Docker network.  Kafka's BROKER listener is fixed
    on internal port 9092 and is reachable by any container that shares the
    network and knows the Kafka container's DNS alias.

    Example::

        from testcontainers.core.network import Network
        from tests.lib.testcontainers.kafka import KafkaContainer, KafkaConnectContainer

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

    # Kafka's BROKER listener is always bound to this port inside Docker.
    # This is distinct from the PLAINTEXT listener (9093) that gets
    # host-port-mapped and returned by get_bootstrap_server().
    KAFKA_INTERNAL_PORT = 9092

    def get_internal_bootstrap_server(self) -> str:
        """Return the Kafka bootstrap address reachable from within the Docker network.

        Uses the first network alias registered via ``.with_network_aliases()``.
        Pass the result directly as the ``bootstrap_servers`` argument to
        ``KafkaConnectContainer``, ``KSQLDbContainer``, or ``SchemaRegistryContainer``.

        Raises:
            RuntimeError: If no network alias has been set on this container.
        """
        if not self._network_aliases:
            raise RuntimeError(
                "No network alias set on KafkaContainer. "
                "Call .with_network_aliases('broker') (or any name) before "
                "calling get_internal_bootstrap_server()."
            )
        return f"{self._network_aliases[0]}:{self.KAFKA_INTERNAL_PORT}"
