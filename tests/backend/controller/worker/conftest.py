# Set required env vars before any module-level settings objects or the Celery
# app are instantiated.  Order matters: os.environ must be populated before
# any import that touches WorkerSettings or creates a DatabaseBackend.
import os

os.environ.setdefault("S3_ENDPOINT", "test-minio:9000")
os.environ.setdefault("S3_ACCESS_KEY", "minioadmin")
os.environ.setdefault("S3_SECRET_KEY", "minioadmin")
os.environ.setdefault("S3_SECURE", "false")
os.environ.setdefault("RABBITMQ_USER", "guest")
os.environ.setdefault("RABBITMQ_PASSWORD", "guest")
os.environ.setdefault("RABBITMQ_HOST", "localhost")
os.environ.setdefault("RABBITMQ_PORT", "5672")
os.environ.setdefault("RABBITMQ_VHOST", "/")
os.environ.setdefault("SQL_DB_HOST", "localhost")
os.environ.setdefault("SQL_DB_PORT", "5432")
os.environ.setdefault("SQL_DB_USER", "postgres")
os.environ.setdefault("SQL_DB_PASSWORD", "postgres")
os.environ.setdefault("SQL_DB_DATABASE", "artemis_test")
os.environ.setdefault("SQL_DRIVER", "postgresql+psycopg2")
os.environ.setdefault("PARSING_SERVICE_URL", "http://test-parsing:10001")
os.environ.setdefault("INGESTION_SERVICE_URL", "http://test-indexing:10000")
os.environ.setdefault("EXCHANGE_NAME", "test-exchange")

# Override the Celery app config BEFORE tasks.py is imported.
#
# tasks.py creates `_db_backend = DatabaseBackend(...)` at module level.
# DatabaseBackend.__init__ calls _create_tables() when
# `database_create_tables_at_setup=True` (set in celery.py).  _create_tables()
# opens a real DB connection — which fails with a fake host.
#
# By forcing `database_create_tables_at_setup=False` here (after celery.py is
# imported but before tasks.py), the DatabaseBackend is safely instantiated
# without touching the network.
from src.backend.controller.worker.celery import app  # noqa: E402

app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    database_create_tables_at_setup=False,
)

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def reset_circuit_breakers():
    """Reset both circuit breakers to CLOSED before and after every test.

    The breakers are module-level singletons — without this fixture a test
    that trips a breaker would leave it OPEN, causing the next unrelated test
    to fail with CircuitBreakerError instead of its own assertion error.
    """
    from src.backend.controller.worker.utils import indexing_breaker, parsing_breaker

    parsing_breaker.close()
    indexing_breaker.close()
    yield
    parsing_breaker.close()
    indexing_breaker.close()
