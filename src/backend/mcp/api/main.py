from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

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

app.include_router(health_router, prefix="/health")
app.mount("/", _mcp_asgi)
