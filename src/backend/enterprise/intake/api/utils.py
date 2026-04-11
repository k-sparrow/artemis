import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from requests.exceptions import HTTPError

from src.backend.enterprise.intake.api.config import settings
from src.backend.enterprise.intake.api.dependencies import kafka_connect_client
from src.backend.enterprise.intake.api.templates import render_http_sink_connector
from src.lib.backend.logging import configure_logging, get_logger

_POLL_INTERVAL = 3  # seconds between status checks
_STARTUP_TIMEOUT = 120  # seconds to wait for connector to reach RUNNING


def _upsert_sink_connector() -> None:
    """Idempotently deploy the HTTP sink connector via PUT /connectors/{name}/config.

    Uses the kafka-connect-py client (synchronous — called inside asyncio.to_thread).
    """
    config = render_http_sink_connector(
        connector_name=settings.HTTP_SINK_CONNECTOR_NAME,
        intake_url=settings.INTAKE_URL,
    )
    # PUT is idempotent: creates if absent, updates if present.
    kafka_connect_client.update_connector(
        settings.HTTP_SINK_CONNECTOR_NAME,
        config["config"],
    )


def _wait_sink_running(timeout: int = _STARTUP_TIMEOUT) -> None:
    """Poll connector status until RUNNING or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = kafka_connect_client.get_connector_status(
                settings.HTTP_SINK_CONNECTOR_NAME
            )
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                time.sleep(_POLL_INTERVAL)
                continue
            raise
        tasks = status.get("tasks", [])
        if (
            status.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t["state"] == "RUNNING" for t in tasks)
        ):
            return
        time.sleep(_POLL_INTERVAL)
    raise TimeoutError(
        f"HTTP sink connector {settings.HTTP_SINK_CONNECTOR_NAME!r} did not reach "
        f"RUNNING within {timeout}s"
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        json_output=not settings.DEBUG,
        include_otel_context=True,
    )
    logger = get_logger("enterprise_intake")
    logger.info("lifespan_started", storage_url=settings.STORAGE_SERVICE_URL)

    logger.info(
        "sink_connector_upsert",
        connector=settings.HTTP_SINK_CONNECTOR_NAME,
        intake_url=settings.INTAKE_URL,
    )
    await asyncio.to_thread(_upsert_sink_connector)

    logger.info("sink_connector_waiting", connector=settings.HTTP_SINK_CONNECTOR_NAME)
    await asyncio.to_thread(_wait_sink_running)

    logger.info("lifespan_ready", connector=settings.HTTP_SINK_CONNECTOR_NAME)
    yield

    logger.info("lifespan_ended")
