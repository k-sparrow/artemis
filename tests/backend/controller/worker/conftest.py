# Set required env vars before any module-level settings objects or the
# Celery app are instantiated.
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

# Celery's own `app.backend` is a lazy property (celery/app/base.py) — merely
# importing celery.py/tasks.py never constructs a backend or opens a DB
# connection, so (unlike the old custom-DatabaseBackend-at-module-level
# design) there is nothing to defuse here beyond eager task execution.
from src.backend.controller.worker.celery import app  # noqa: E402

app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
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
