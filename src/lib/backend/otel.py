# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

import functools
import os
from typing import Callable

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

__all__ = ["setup_telemetry", "with_telemetry"]


def setup_telemetry(service_name: str) -> None:
    """Configure TracerProvider + OTLP exporter.

    No-op when OTEL_EXPORTER_OTLP_ENDPOINT is unset or empty, so existing
    tests and local dev runs without the observability profile are unaffected.
    """
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    if not endpoint:
        return

    resource = Resource(attributes={SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
    SQLAlchemyInstrumentor().instrument()


def with_telemetry(service_name: str) -> Callable:
    """Decorator for FastAPI lifespan functions.

    Calls setup_telemetry(service_name) before entering the lifespan context so
    the TracerProvider and instrumentors are active for the entire app lifetime.

    Usage::

        @with_telemetry("backend-storage")
        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncIterator[None]:
            ...
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            setup_telemetry(service_name)
            return fn(*args, **kwargs)

        return wrapper

    return decorator
