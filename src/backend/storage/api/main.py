from fastapi import FastAPI

from src.backend.storage.api.exceptions import (
    register_custom_exception_handlers,
    EXCEPTION_HANDLER_MAP,
)
from src.backend.storage.api.utils import lifespan
from src.backend.storage.api.private import router as private_router
from src.backend.storage.api.health import router as health_router

app = FastAPI(
    title="Artemis Storage Service",
    description="S3 object storage for Artemis project",
    lifespan=lifespan,
)

register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAP)

app.include_router(health_router, prefix="/health")
app.include_router(private_router, prefix="/private")
