# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

import logging
import os

import httpx  # noqa: F401
import sqlalchemy  # noqa: F401

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

__all__ = ["is_telemetry_enabled", "setup_telemetry"]

_log = logging.getLogger(__name__)


def is_telemetry_enabled() -> bool:
    return bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))


def setup_telemetry(service_name: str, *instrumentors: BaseInstrumentor) -> None:
    """Configure TracerProvider + OTLP exporter, then run the given instrumentors.

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

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint, insecure=True)
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    for instrumentor in instrumentors:
        instrumentor.instrument()

    _log.info(
        "OTel tracing + metrics configured",
        extra={"service": service_name, "endpoint": endpoint},
    )
