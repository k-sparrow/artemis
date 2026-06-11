from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from src.lib.backend.api.exceptions import register_custom_exception_handlers
from src.lib.backend.telemetry import is_telemetry_enabled, setup_telemetry
from src.backend.storage.api import exceptions as shared_exc
from src.backend.storage.api.namespaces import exceptions as ns_exc
from src.backend.storage.api.files import exceptions as files_exc
from src.backend.storage.api.utils import lifespan

# API routers
from src.backend.storage.api.health import router as health_router
from src.backend.storage.api.namespaces.router import router as namespaces_router
from src.backend.storage.api.files.router import router as files_router

app = FastAPI(
    title="Artemis Storage Service",
    description="S3 object storage for Artemis project",
    lifespan=lifespan,
)

# Instrument at construction time — FastAPIInstrumentor must wrap the app before
# Starlette builds its middleware stack (which happens on the first request /
# lifespan event), otherwise no HTTP request spans are emitted.
if is_telemetry_enabled():
    setup_telemetry(
        "backend-storage",
        HTTPXClientInstrumentor(),
        SQLAlchemyInstrumentor(),
    )
    FastAPIInstrumentor.instrument_app(app)

register_custom_exception_handlers(
    app,
    {
        **shared_exc.EXCEPTION_HANDLER_MAP,
        **ns_exc.EXCEPTION_HANDLER_MAP,
        **files_exc.EXCEPTION_HANDLER_MAP,
    },
)

app.include_router(health_router, prefix="/health")
app.include_router(namespaces_router, prefix="/namespaces")
app.include_router(files_router, prefix="/namespaces")
