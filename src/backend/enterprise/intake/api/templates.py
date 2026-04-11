"""Kafka Connect HTTP sink connector template for the enterprise intake service.

The HTTP sink connector is a singleton infrastructure resource owned by the
intake service.  It consumes from ``artemis.datasource.filesystem.intake``
(a ksqlDB-reshaped topic) and POSTs each record as a JSON body to ``POST /intake``.

Connector class: ``io.aiven.kafka.connect.http.HttpSinkConnector``
(Aiven HTTP Sink — already installed in the artemis/cp-kafka-connect image)

Message format
--------------
The upstream ksqlDB transformation reads ``artemis.datasource.filesystem``
(Camel FileSource messages with artemis.* headers) and produces
``artemis.datasource.filesystem.intake`` with values matching the
``IntakeRequest`` schema::

    {
      "source":       {"type": "filesystem", "path": "/watch/doc.txt"},
      "display_name": "doc.txt",
      "content_type": "application/octet-stream",
      "namespace_id": "<uuid>"
    }

Because the message value is already shaped correctly by ksqlDB, the HTTP
sink needs no SMT chain — it POSTs the JSON value verbatim.
"""

from __future__ import annotations

from typing import Any

_FILESYSTEM_INTAKE_TOPIC = "artemis.datasource.filesystem.intake"


def render_http_sink_connector(
    *,
    connector_name: str,
    intake_url: str,
) -> dict[str, Any]:
    """Render the Aiven HTTP sink connector config for the enterprise intake pipeline.

    Args:
        connector_name: Kafka Connect connector name (from settings).
        intake_url:     Full URL of the intake endpoint
                        (e.g. ``http://enterprise-intake:9000/intake``).

    Returns:
        Connector config dict for ``KafkaConnect.create_connector()`` /
        ``PUT /connectors/{name}/config``.
    """
    return {
        "name": connector_name,
        "config": {
            "connector.class": "io.aiven.kafka.connect.http.HttpSinkConnector",
            "tasks.max": "1",
            "topics": _FILESYSTEM_INTAKE_TOPIC,
            "http.url": intake_url,
            "http.authorization.type": "none",
            "batching.enabled": False,
            "consumer.override.auto.offset.reset": "latest",
            # ksqlDB produces JSON values — use JsonConverter with no schema envelope.
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        },
    }
