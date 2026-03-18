from celery import Celery
from kombu import Exchange, Queue

from src.backend.controller.worker.config import settings

__all__ = ["app"]

app = Celery(broker=settings.MESSAGE_BROKER_URL)

app.conf.result_serializer = "json"
app.conf.database_create_tables_at_setup = True
app.conf.result_extended = True

# Two queues with different scaling profiles:
#   fetch-and-parse  — GPU-bound (Docling); scale with GPU workers
#   index            — I/O-bound (TEI + Qdrant + Postgres); scale independently
app.conf.task_queues = [
    Queue(
        name="artemis.ingestion.fetch-and-parse",
        exchange=Exchange(settings.EXCHANGE_NAME, type="direct"),
        routing_key="fetch-and-parse",
        durable=True,
    ),
    Queue(
        name="artemis.ingestion.index",
        exchange=Exchange(settings.EXCHANGE_NAME, type="direct"),
        routing_key="index",
        durable=True,
    ),
]

app.conf.task_routes = {
    "tasks.ingest": {
        "queue": "artemis.ingestion.fetch-and-parse",
        "routing_key": "fetch-and-parse",
        "serializer": "json",
    },
    "tasks.fetch_and_parse": {
        "queue": "artemis.ingestion.fetch-and-parse",
        "routing_key": "fetch-and-parse",
        "serializer": "json",
    },
    "tasks.index": {
        "queue": "artemis.ingestion.index",
        "routing_key": "index",
        "serializer": "json",
    },
}
