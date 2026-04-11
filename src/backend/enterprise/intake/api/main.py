from fastapi import FastAPI

from src.lib.backend.api.exceptions import register_custom_exception_handlers
from src.backend.enterprise.intake.api.intake.exceptions import EXCEPTION_HANDLER_MAP
from src.backend.enterprise.intake.api.utils import lifespan

from src.backend.enterprise.intake.api.health import router as health_router
from src.backend.enterprise.intake.api.intake.router import router as intake_router

app = FastAPI(
    title="Artemis Enterprise Ingest Service",
    description=(
        "Bridge between Kafka HTTP sink and the storage service "
        "for enterprise document intake."
    ),
    lifespan=lifespan,
)

register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAP)

app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(intake_router)
