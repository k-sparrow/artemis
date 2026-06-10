import importlib
from unittest.mock import MagicMock

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


def _reload_telemetry_module():
    """Re-import telemetry.setup so setup_telemetry runs fresh in each test."""
    import src.lib.backend.telemetry.otel as mod

    importlib.reload(mod)
    return mod


def test_is_telemetry_enabled(monkeypatch):
    mod = _reload_telemetry_module()
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    assert mod.is_telemetry_enabled() is False
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    assert mod.is_telemetry_enabled() is True


def test_noop_when_no_endpoint(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    mod = _reload_telemetry_module()
    instrumentor = MagicMock()
    mod.setup_telemetry("test-service", instrumentor)
    # No TracerProvider should have been installed; proxy provider is the default
    provider = trace.get_tracer_provider()
    assert not isinstance(provider, TracerProvider)
    instrumentor.instrument.assert_not_called()


def test_configures_provider_when_endpoint_set(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    mod = _reload_telemetry_module()
    instrumentor = MagicMock()
    try:
        mod.setup_telemetry("test-service", instrumentor)
        provider = trace.get_tracer_provider()
        assert isinstance(provider, TracerProvider)
        assert len(provider._active_span_processor._span_processors) > 0
        instrumentor.instrument.assert_called_once()
    finally:
        # Reset global tracer provider so other tests aren't affected
        from opentelemetry.sdk.trace import TracerProvider as _TP  # noqa: F401

        trace._TRACER_PROVIDER = None  # noqa: SLF001
