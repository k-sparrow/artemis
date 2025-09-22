import logging
import logging.config
from typing import Dict

from fastapi import FastAPI, Request, APIRouter

from src.backend.storage.api.config import LOGGING_CONFIG
from src.backend.storage.api.exceptions import (
    register_custom_exception_handlers,
    EXCEPTION_HANDLER_MAP,
)
from src.backend.storage.api.utils import (
    lifespan,
)
from src.backend.storage.api.private import router as private_router
from src.backend.storage.api.health import router as health_router

API_ROUTERS: Dict[str, APIRouter] = {
    "/health": health_router,
    "/private": private_router,
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# define the FastAPI application
app = FastAPI(
    title="Venus Storage Service",
    description="S3 object storage for Venus project",
    lifespan=lifespan,
)

# specify HTTP response codes for user-defined exceptions
register_custom_exception_handlers(app, EXCEPTION_HANDLER_MAP)

# add routers for the app
for path_prefix, router in API_ROUTERS.items():
    app.include_router(router, prefix=path_prefix)

# here arrive update notifications from Kafka
# about fresh uploads to the
@app.post("/library")
async def ee(request: Request):
    logger.info(f"{request.method} {request.url}")
    logger.info(f"Request contents: {await request.body()}")
    return {"message": "Hello from Venus!"}
