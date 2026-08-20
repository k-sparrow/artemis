"""Plain SQLAlchemy session factory, shared by claims.py and outbox.py.

Deliberately independent of Celery's own result backend (see celery.py) —
claims.py/outbox.py write to tables we own (parse_stage_state,
ingestion_status), unrelated to whatever Celery does with its own
celery_taskmeta/celery_tasksetmeta tables (the official, unmodified
db+ backend recipe — no custom subclass needed there any more).
"""

from __future__ import annotations

import functools
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.backend.controller.worker.config import settings

__all__ = ["SessionLocal", "session_cleanup"]


@functools.lru_cache(maxsize=1)
def _get_engine():
    return create_engine(settings.BACKEND_RESULT_URL, echo=False)


def SessionLocal() -> Session:
    """Returns a fresh Session, lazily constructing the engine on first call.

    create_engine() itself imports the DBAPI driver module matching the URL
    scheme — doing that at module import time (like the old custom
    DatabaseBackend did) breaks unit tests that never touch this module's DB
    access at all, since they don't necessarily have a real driver installed
    for whatever placeholder SQL_DRIVER value their env sets.
    """
    return sessionmaker(bind=_get_engine())()


@contextmanager
def session_cleanup(session: Session):
    try:
        yield
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
