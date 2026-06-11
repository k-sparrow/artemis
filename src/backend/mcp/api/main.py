from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from src.lib.backend.telemetry import is_telemetry_enabled, setup_telemetry
from src.backend.mcp.api.health.router import router as health_router
from src.backend.mcp.api.tools import mcp
from src.backend.mcp.api.utils import lifespan as _base_lifespan

# Trigger lazy session-manager creation so mcp.session_manager is available
# before the app starts.
_mcp_asgi = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with _base_lifespan(app):
        async with mcp.session_manager.run():
            yield


app = FastAPI(title="Artemis MCP Server", lifespan=lifespan)

# Instrument at construction time — before Starlette builds its middleware stack.
if is_telemetry_enabled():
    setup_telemetry(
        "backend-mcp",
        HTTPXClientInstrumentor(),
        SQLAlchemyInstrumentor(),
    )
    FastAPIInstrumentor.instrument_app(app)

app.include_router(health_router, prefix="/health")
app.mount("/", _mcp_asgi)
