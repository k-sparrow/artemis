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
