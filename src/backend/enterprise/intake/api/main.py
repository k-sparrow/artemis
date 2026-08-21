import logging

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from src.lib.backend.api.exceptions import register_custom_exception_handlers
from src.lib.backend.logging import get_logger
from src.lib.backend.telemetry import is_telemetry_enabled, setup_telemetry
from src.backend.enterprise.intake.api.intake.exceptions import EXCEPTION_HANDLER_MAP
from src.backend.enterprise.intake.api.utils import lifespan

from src.backend.enterprise.intake.api.health import router as health_router
from src.backend.enterprise.intake.api.intake.router import router as intake_router

_log = get_logger(__name__)


class _SuppressUnsupportedUpgradeWarning(logging.Filter):
    """The Aiven HTTP sink connector (the only real client that calls
    /intake) builds its java.net.http.HttpClient with no explicit
    `.version(...)`, so it defaults to HTTP_2 and attempts an h2c cleartext
    upgrade on every fresh connection — confirmed by inspecting
    HttpSenderFactory.buildHttpClient() in the connector's own JAR, not
    guessed. uvicorn logs "Unsupported upgrade request." for that on
    uvicorn.error, since h2c isn't a WebSocket upgrade regardless of whether
    a WS library is installed (wsproto is, for genuine WS clients — see
    BUILD.bazel — but it can't matter here: uvicorn only treats an upgrade
    as supported when the Upgrade header value is literally "websocket").
    Nothing on our side or the connector's config can prevent the h2c
    attempt itself, so this filters the resulting log line specifically;
    every other uvicorn.error message (real errors, startup info) is
    unaffected.
    """

    _SUPPRESSED_PREFIX = "Unsupported upgrade request."

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.getMessage().startswith(self._SUPPRESSED_PREFIX)


logging.getLogger("uvicorn.error").addFilter(_SuppressUnsupportedUpgradeWarning())

app = FastAPI(
    title="Artemis Enterprise Ingest Service",
    description=(
        "Bridge between Kafka HTTP sink and the storage service "
        "for enterprise document intake."
    ),
    lifespan=lifespan,
)

# Instrument at construction time — before Starlette builds its middleware stack.
if is_telemetry_enabled():
    setup_telemetry(
        "backend-enterprise-intake",
        HTTPXClientInstrumentor(),
        SQLAlchemyInstrumentor(),
    )
    FastAPIInstrumentor.instrument_app(app)

register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAP)


@app.exception_handler(RequestValidationError)
async def _log_validation_error(
    req: Request, exc: RequestValidationError
) -> JSONResponse:
    try:
        body = await req.body()
        body_text = body.decode("utf-8", errors="replace")
    except Exception:
        body_text = "<unreadable>"
    _log.warning(
        "intake_validation_error",
        errors=exc.errors(),
        raw_body=body_text,
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )


app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(intake_router)
