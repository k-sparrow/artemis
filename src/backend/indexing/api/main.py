from fastapi import FastAPI

from src.lib.backend.api import register_custom_exception_handlers
from src.backend.indexing.api.exceptions import EXCEPTION_HANDLER_MAPPING
from src.backend.indexing.api.utils import lifespan
from src.backend.indexing.api.health import router as health_router
from src.backend.indexing.api.index import router as index_router
from src.backend.indexing.api.retrieve import router as retrieve_router


app = FastAPI(
    title="Basic File Ingestion with Celery",
    lifespan=lifespan,
)

register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAPPING)

app.include_router(health_router, prefix="/health")
app.include_router(index_router)
app.include_router(retrieve_router)
