import importlib
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


def _reload_otel_module():
    """Re-import otel.py so setup_telemetry runs fresh in each test."""
    import src.lib.backend.otel as mod

    importlib.reload(mod)
    return mod


def test_noop_when_no_endpoint(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    mod = _reload_otel_module()
    mod.setup_telemetry("test-service")
    # No TracerProvider should have been installed; proxy provider is the default
    provider = trace.get_tracer_provider()
    assert not isinstance(provider, TracerProvider)


def test_configures_provider_when_endpoint_set(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    mod = _reload_otel_module()
    try:
        mod.setup_telemetry("test-service")
        provider = trace.get_tracer_provider()
        assert isinstance(provider, TracerProvider)
        assert len(provider._active_span_processor._span_processors) > 0
    finally:
        # Reset global tracer provider so other tests aren't affected
        from opentelemetry.sdk.trace import TracerProvider as _TP

        trace._TRACER_PROVIDER = None  # noqa: SLF001
