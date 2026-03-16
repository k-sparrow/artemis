# third party
from celery import Celery
from kombu import Exchange, Queue

# our code
from src.backend.controller.worker.config import settings

__all__ = [
    "app",
]

app = Celery(
    broker=settings.MESSAGE_BROKER_URL,
    # result backend will be declared for each task
    # as it is custom
)

# app.conf.database_engine_options = {'echo': True}
app.conf.result_serializer = "json"
app.conf.database_create_tables_at_setup = True
app.conf.result_extended = True

app.conf.task_queues = [
    Queue(
        name=f"apollo.ingestion.{typename}",
        exchange=Exchange(settings.EXCHANGE_NAME, type="direct"),
        routing_key=typename,
        durable=False,
    )
    for typename in ["library", "private"]
]

app.conf.task_routes = {
    f"tasks.ingestion.{typename}": {
        "queue": f"apollo.ingestion.{typename}",
        "routing_key": typename,
        "serializer": "json",
    }
    for typename in ["library", "private"]
}
