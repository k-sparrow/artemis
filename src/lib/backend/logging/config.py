# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#

import logging

import structlog

from src.lib.backend.logging.processors import inject_otel_context

__all__ = [
    "configure_logging",
    "get_logger",
]


def configure_logging(
    level: int = logging.INFO,
    json_output: bool = True,
    include_otel_context: bool = True,
):
    """
    Configure structlog for production use.

    Args:
        level: Minimum log level
        json_output: If True, output JSON; if False, output colored console format
        include_otel_context: If True, inject trace_id/span_id from OTel
    """
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if include_otel_context:
        processors.insert(1, inject_otel_context)

    if json_output:
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Dev-friendly colored output
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None) -> structlog.BoundLogger:
    """Get a structlog logger, optionally bound to a name."""
    logger = structlog.get_logger()
    if name:
        logger = logger.bind(logger_name=name)
    return logger
