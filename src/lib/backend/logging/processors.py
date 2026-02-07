# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#

from opentelemetry import trace

__all__ = [
    "inject_otel_context",
]


def inject_otel_context(logger, method_name, event_dict):
    """Inject OpenTelemetry trace_id and span_id into log events."""
    span = trace.get_current_span()
    if span.is_recording():
        ctx = span.get_span_context()
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict
