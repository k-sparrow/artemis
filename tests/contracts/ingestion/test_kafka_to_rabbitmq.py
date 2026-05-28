"""Contract test: artemis.ingestion.celery.tasks.routed (Kafka) → RabbitMQ queue.

Validates the Camel Spring RabbitMQ Sink Connector SMT chain that routes
Celery task messages from the routed Kafka topic into RabbitMQ so the
Celery worker can consume them.

Contract under test
-------------------
Input  topic : artemis.ingestion.celery.tasks.routed
  key   : JSON struct {"task_id": "<uuid>"}   (ksqlDB PARTITION BY STRUCT output)
  value : raw Celery v2 JSON bytes             (ByteArrayConverter)

SMT chain (mirrors production):
  HeaderFrom$Key  — {"task_id": "<uuid>"} key → AMQP header CamelHeader.id
  InsertHeader    — adds CamelHeader.task = "tasks.ingest"
  InsertHeader    — adds CamelHeader.CamelSpringRabbitmqContentType = "application/json"

Output: RabbitMQ queue artemis.ingestion.fetch-and-parse
  - body     : original Celery v2 JSON bytes, unchanged
  - headers  : CamelHeader.id == task_id (str)
               CamelHeader.task == "tasks.ingest"
               CamelHeader.CamelSpringRabbitmqContentType == "application/json"

Infrastructure: KafkaContainer (KRaft) + KafkaConnectContainer + RabbitMqContainer.
No ksqlDB required — this test covers only the Kafka → RabbitMQ boundary.

Pre-requisites:
    bazel run //tools/oci/images:artemis-kafka-connect-init.cmd
    (artemis/cp-kafka-connect:latest must also be built)
"""

from __future__ import annotations

import json
import time
import uuid

import pika
import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.network import Network
from testcontainers.rabbitmq import RabbitMqContainer

from src.lib.core.ingestion.contract import (
    IngestionInfo,
    IngestionTaskDetails,
    S3Details,
    SourceDetails,
)
from tests.lib.testcontainers.kafka import KafkaConnectContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_ROUTED_TOPIC = "artemis.ingestion.celery.tasks.routed"
_QUEUE = "artemis.ingestion.fetch-and-parse"
_EXCHANGE = "amq.direct"
_ROUTING_KEY = "fetch-and-parse"
_CONNECTOR_NAME = "CamelRabbitMQSinkConnector__CeleryTasksIngestion__ContractTest"

_RABBITMQ_USER = "guest"
_RABBITMQ_PASS = "guest"
_RABBITMQ_VHOST = "/"


# ---------------------------------------------------------------------------
# Session fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def network():
    with Network() as n:
        yield n


@pytest.fixture(scope="module")
def kafka(network: Network) -> KafkaContainer:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(network)
        .with_network_aliases("broker")
        .with_kwargs(hostname="broker")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def bootstrap_server(kafka: KafkaContainer) -> str:
    return kafka.get_bootstrap_server()


@pytest.fixture(scope="module")
def routed_topic(bootstrap_server: str) -> str:
    admin = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(_ROUTED_TOPIC, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()
    return _ROUTED_TOPIC


@pytest.fixture(scope="module")
def rabbitmq(network: Network) -> RabbitMqContainer:
    container = (
        RabbitMqContainer(image="rabbitmq:4.1.2-management-alpine")
        .with_network(network)
        .with_network_aliases("rabbitmq")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def rabbitmq_params(rabbitmq: RabbitMqContainer) -> pika.ConnectionParameters:
    host = rabbitmq.get_container_host_ip()
    port = int(rabbitmq.get_exposed_port(5672))
    return pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=_RABBITMQ_VHOST,
        credentials=pika.PlainCredentials(_RABBITMQ_USER, _RABBITMQ_PASS),
    )


@pytest.fixture(scope="module")
def declared_queue(rabbitmq_params: pika.ConnectionParameters) -> str:
    """Pre-declare the queue so the connector (autoDeclareProducer=false) can publish."""
    conn = pika.BlockingConnection(rabbitmq_params)
    ch = conn.channel()
    ch.queue_declare(queue=_QUEUE, durable=True)
    ch.queue_bind(queue=_QUEUE, exchange=_EXCHANGE, routing_key=_ROUTING_KEY)
    conn.close()
    return _QUEUE


@pytest.fixture(scope="module")
def kafka_connect(network: Network, kafka: KafkaContainer) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(network)
        .with_network_aliases("kafka-connect")
        .with_log_level("WARN")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def rabbitmq_sink_connector(
    kafka_connect: KafkaConnectContainer,
    rabbitmq: RabbitMqContainer,
    routed_topic: str,
    declared_queue: str,
) -> None:
    """Deploy the Camel RabbitMQ sink connector with the production SMT chain."""
    import httpx

    config = {
        "connector.class": (
            "org.apache.camel.kafkaconnector.springrabbitmqsink"
            ".CamelSpringrabbitmqsinkSinkConnector"
        ),
        "tasks.max": "1",
        "topics": _ROUTED_TOPIC,
        "camel.kamelet.spring-rabbitmq-sink.host": "rabbitmq",
        "camel.kamelet.spring-rabbitmq-sink.port": "5672",
        "camel.kamelet.spring-rabbitmq-sink.username": _RABBITMQ_USER,
        "camel.kamelet.spring-rabbitmq-sink.password": _RABBITMQ_PASS,
        "camel.kamelet.spring-rabbitmq-sink.vhost": _RABBITMQ_VHOST,
        "camel.kamelet.spring-rabbitmq-sink.exchangeName": _EXCHANGE,
        "camel.kamelet.spring-rabbitmq-sink.routingKey": _ROUTING_KEY,
        "camel.kamelet.spring-rabbitmq-sink.queues": _QUEUE,
        "camel.kamelet.spring-rabbitmq-sink.autoDeclareProducer": "false",
        # ByteArrayConverter: passes raw JSON value bytes unchanged to Camel
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "consumer.override.auto.offset.reset": "latest",
        # SMT chain — mirrors production exactly
        "transforms": "moveTaskId,insertTaskNameHeader,insertContentTypeHeader",
        "transforms.moveTaskId.type": (
            "org.apache.kafka.connect.transforms.HeaderFrom$Key"
        ),
        "transforms.moveTaskId.fields": "task_id",
        "transforms.moveTaskId.headers": "CamelHeader.id",
        "transforms.moveTaskId.operation": "move",
        "transforms.insertTaskNameHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.insertTaskNameHeader.header": "CamelHeader.task",
        "transforms.insertTaskNameHeader.value.literal": "tasks.ingest",
        "transforms.insertContentTypeHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.insertContentTypeHeader.header": (
            "CamelHeader.CamelSpringRabbitmqContentType"
        ),
        "transforms.insertContentTypeHeader.value.literal": "application/json",
    }

    kc_url = kafka_connect.get_url()
    with httpx.Client(base_url=kc_url, timeout=15.0) as client:
        resp = client.put(f"/connectors/{_CONNECTOR_NAME}/config", json=config)
        resp.raise_for_status()

    deadline = time.monotonic() + 90
    with httpx.Client(base_url=kc_url, timeout=10.0) as client:
        while time.monotonic() < deadline:
            r = client.get(f"/connectors/{_CONNECTOR_NAME}/status")
            if r.is_success:
                s = r.json()
                tasks = s.get("tasks", [])
                if (
                    s.get("connector", {}).get("state") == "RUNNING"
                    and tasks
                    and all(t["state"] == "RUNNING" for t in tasks)
                ):
                    return
            time.sleep(2)
    pytest.fail(f"Connector {_CONNECTOR_NAME!r} did not reach RUNNING within 90s")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task_details() -> IngestionTaskDetails:
    namespace_id = uuid.uuid4()
    return IngestionTaskDetails(
        upload_action="CREATE",
        s3=S3Details(
            bucket="artemis",
            object=f"{namespace_id}/report.pdf",
            size=204800,
        ),
        source=SourceDetails(
            source=f"{namespace_id}/report.pdf",
            content_type="application/pdf",
            obj_id=uuid.uuid4(),
            object_type="file",
        ),
        info=IngestionInfo(namespace_id=namespace_id, group_id=uuid.uuid4()),
    )


def _produce_routed_message(
    bootstrap_server: str, task_id: str, details: IngestionTaskDetails
) -> None:
    """Produce a Celery v2 message to the routed topic."""
    value = json.dumps(
        {"args": [], "kwargs": json.loads(details.model_dump_json())}
    ).encode()
    key = json.dumps({"task_id": task_id}).encode()

    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    producer.send(_ROUTED_TOPIC, key=key, value=value)
    producer.flush()
    producer.close()


def _consume_from_rabbitmq(
    params: pika.ConnectionParameters,
    timeout_s: int = 30,
) -> pika.spec.Basic.Deliver | None:
    """Poll the queue until a message arrives or timeout; return (method, props, body)."""
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        method, props, body = ch.basic_get(queue=_QUEUE, auto_ack=True)
        if method:
            conn.close()
            return method, props, body
        time.sleep(0.5)
    conn.close()
    return None, None, None


# ---------------------------------------------------------------------------
# Contract assertions
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCamelRabbitMQSinkConnector:
    """Camel RabbitMQ sink connector delivers messages from Kafka to RabbitMQ.

    Uses the exact SMT chain from production:
      HeaderFrom$Key  → AMQP header 'id' (task UUID for Celery v2 protocol)
      InsertHeader    → AMQP header 'task' = "tasks.ingest"
      InsertHeader    → AMQP BasicProperties.content_type = "application/json"

    Note: Camel strips the 'CamelHeader.' prefix when writing to AMQP. The Kafka
    Connect headers 'CamelHeader.id' and 'CamelHeader.task' arrive in RabbitMQ as
    'id' and 'task' respectively. 'CamelHeader.CamelSpringRabbitmqContentType' is
    mapped to the AMQP content_type property rather than the headers table.
    """

    def test_message_arrives_in_queue(
        self,
        bootstrap_server: str,
        rabbitmq_sink_connector: None,
        rabbitmq_params: pika.ConnectionParameters,
    ) -> None:
        task_id = str(uuid.uuid4())
        _produce_routed_message(bootstrap_server, task_id, _make_task_details())
        _, _, body = _consume_from_rabbitmq(rabbitmq_params)
        assert body is not None, f"No message arrived on {_QUEUE!r} within timeout"

    def test_ingestion_task_details_survive_transit(
        self,
        bootstrap_server: str,
        rabbitmq_sink_connector: None,
        rabbitmq_params: pika.ConnectionParameters,
    ) -> None:
        """The full IngestionTaskDetails kwargs must arrive byte-identical in RabbitMQ.

        This is the core contract: the storage service constructs IngestionTaskDetails,
        it travels through MinIO metadata → Kafka → ksqlDB → Camel connector →
        RabbitMQ, and the Celery worker must be able to deserialise it unchanged.
        A field rename or type coercion anywhere in the pipeline would break this.
        """
        task_id = str(uuid.uuid4())
        original = _make_task_details()
        _produce_routed_message(bootstrap_server, task_id, original)

        _, _, body = _consume_from_rabbitmq(rabbitmq_params)
        assert body is not None, "No message arrived"

        parsed = json.loads(body.decode())
        assert parsed["args"] == []
        recovered = IngestionTaskDetails.model_validate(parsed["kwargs"])
        assert recovered == original

    def test_task_id_promoted_to_amqp_header_id(
        self,
        bootstrap_server: str,
        rabbitmq_sink_connector: None,
        rabbitmq_params: pika.ConnectionParameters,
    ) -> None:
        """HeaderFrom$Key must extract task_id from the struct key → AMQP header 'id'.

        Camel strips the 'CamelHeader.' prefix when writing to AMQP, so the
        Kafka Connect header 'CamelHeader.id' arrives in RabbitMQ as 'id'.
        """
        task_id = str(uuid.uuid4())
        _produce_routed_message(bootstrap_server, task_id, _make_task_details())
        _, props, body = _consume_from_rabbitmq(rabbitmq_params)
        assert body is not None, "No message arrived"
        headers = props.headers or {}
        assert headers.get("id") == task_id

    def test_task_name_header_is_tasks_ingest(
        self,
        bootstrap_server: str,
        rabbitmq_sink_connector: None,
        rabbitmq_params: pika.ConnectionParameters,
    ) -> None:
        """InsertHeader SMT must add AMQP header 'task' = 'tasks.ingest'.

        Camel strips the 'CamelHeader.' prefix; 'CamelHeader.task' arrives as 'task'.
        """
        task_id = str(uuid.uuid4())
        _produce_routed_message(bootstrap_server, task_id, _make_task_details())
        _, props, body = _consume_from_rabbitmq(rabbitmq_params)
        assert body is not None, "No message arrived"
        headers = props.headers or {}
        assert headers.get("task") == "tasks.ingest"

    def test_content_type_header_is_application_json(
        self,
        bootstrap_server: str,
        rabbitmq_sink_connector: None,
        rabbitmq_params: pika.ConnectionParameters,
    ) -> None:
        """InsertHeader SMT must set the AMQP content_type property to 'application/json'.

        Camel maps 'CamelHeader.CamelSpringRabbitmqContentType' to the AMQP
        BasicProperties.content_type field, not to the application headers table.
        """
        task_id = str(uuid.uuid4())
        _produce_routed_message(bootstrap_server, task_id, _make_task_details())
        _, props, body = _consume_from_rabbitmq(rabbitmq_params)
        assert body is not None, "No message arrived"
        assert props.content_type == "application/json"
