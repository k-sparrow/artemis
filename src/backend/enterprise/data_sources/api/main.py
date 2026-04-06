from fastapi import FastAPI

from src.lib.backend.api.exceptions import register_custom_exception_handlers
from src.backend.enterprise.data_sources.api.sources.exceptions import (
    EXCEPTION_HANDLER_MAP,
)
from src.backend.enterprise.data_sources.api.utils import lifespan

from src.backend.enterprise.data_sources.api.health import router as health_router
from src.backend.enterprise.data_sources.api.sources.router import (
    router as sources_router,
)

app = FastAPI(
    title="Artemis Data Sources",
    description=(
        "Control plane for Kafka Connect source connectors. "
        "Registers data sources, deploys connectors, and manages their lifecycle."
    ),
    lifespan=lifespan,
)

register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAP)

app.include_router(health_router, prefix="/health")
app.include_router(sources_router)
