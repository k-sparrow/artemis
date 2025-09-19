import logging
import logging.config
from uuid import UUID
from typing import List

from fastapi import FastAPI, File, UploadFile, Request, Form, status

from src.backend.storage.api.config import LOGGING_CONFIG
from src.backend.storage.api.dependencies import (
    minio_client_dependency,
)
from src.backend.storage.api.schemas import PrivateUploadResponse
import src.backend.storage.api.service as service
from src.backend.storage.api.utils import (
    lifespan,
)

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


app = FastAPI(
    title="Venus Storage Service",
    description="S3 object storage for Venus project",
    lifespan=lifespan,
)


# here arrive update notifications from Kafka
# about fresh uploads to the
@app.post("/library")
async def ee(request: Request):
    logger.info(f"{request.method} {request.url}")
    logger.info(f"Request contents: {await request.body()}")
    return {"message": "Hello from Venus!"}


@app.post(
    "/upload",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PrivateUploadResponse,
)
def upload(
    minio_client: minio_client_dependency,
    files: List[UploadFile] = File(...),
    user_id: UUID = Form(),
    chat_id: UUID = Form(),
):
    return service.upload(
        minio_client=minio_client,
        files=files,
        user_id=user_id,
        chat_id=chat_id,
    )
