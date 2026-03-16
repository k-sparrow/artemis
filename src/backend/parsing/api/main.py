from fastapi import FastAPI

from src.lib.backend.api import register_custom_exception_handlers
from src.backend.parsing.api.exceptions import EXCEPTION_HANDLER_MAPPING
from src.backend.parsing.api.utils import lifespan
from src.backend.parsing.api.health import router as health_router
from src.backend.parsing.api.parse import router as parse_router


app = FastAPI(
    title="Document Parsing Service",
    lifespan=lifespan,
)

register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAPPING)

app.include_router(health_router, prefix="/health")
app.include_router(parse_router)
