from celery import Celery
from celery.signals import worker_process_init
from kombu import Exchange, Queue
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from src.backend.controller.worker.config import settings
from src.lib.backend.telemetry import is_telemetry_enabled, setup_telemetry


@worker_process_init.connect(weak=False)
def _setup_otel(**kwargs) -> None:
    if is_telemetry_enabled():
        setup_telemetry(
            "controller-worker",
            HTTPXClientInstrumentor(),
            SQLAlchemyInstrumentor(),
            CeleryInstrumentor(),
        )


__all__ = ["app"]

app = Celery(
    broker=settings.MESSAGE_BROKER_URL,
    # Result backend is declared per-task (backend=_db_backend in tasks.py)
    # using our custom DatabaseBackend. No app-level backend is set here.
)

app.conf.result_serializer = "json"
app.conf.database_create_tables_at_setup = True
app.conf.result_extended = True

# Workers are split into two pools with different scaling profiles:
#   parse  — Celery poll loop (async, non-blocking); concurrency=3
#   index  — I/O-bound (TEI + Qdrant + Postgres); concurrency=1
# prefetch=1 prevents any single worker from hoarding multiple tasks.
app.conf.worker_prefetch_multiplier = 1
# Global default: tasks ack on receipt so failed workers don't leave ghosts.
# Tasks that must be redelivered on worker crash override to acks_late=True.
app.conf.task_acks_late = False

app.conf.task_queues = [
    Queue(
        name="artemis.ingestion.parse",
        exchange=Exchange(settings.EXCHANGE_NAME, type="direct"),
        routing_key="parse",
        durable=True,
    ),
    Queue(
        name="artemis.ingestion.index",
        exchange=Exchange(settings.EXCHANGE_NAME, type="direct"),
        routing_key="index",
        durable=True,
    ),
]

_PARSE_ROUTE = {
    "queue": "artemis.ingestion.parse",
    "routing_key": "parse",
    "serializer": "json",
}
_INDEX_ROUTE = {
    "queue": "artemis.ingestion.index",
    "routing_key": "index",
    "serializer": "json",
}

app.conf.task_routes = {
    # New async parse sub-chain
    "tasks.ingest": _PARSE_ROUTE,
    "tasks.parse": _PARSE_ROUTE,
    "tasks.submit_parse": _PARSE_ROUTE,
    "tasks.poll_parse": _PARSE_ROUTE,
    "tasks.resolve_parse": _PARSE_ROUTE,
    "tasks.poll_resolve": _PARSE_ROUTE,
    # Indexing
    "tasks.index": _INDEX_ROUTE,
    "tasks.delete_document": _INDEX_ROUTE,
    "tasks.delete_namespace": _INDEX_ROUTE,
}
