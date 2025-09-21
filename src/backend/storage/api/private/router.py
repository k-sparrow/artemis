from uuid import UUID
from typing import List

from fastapi import APIRouter, status, UploadFile, File, Form

from src.backend.storage.api.dependencies import minio_client_dependency
from src.backend.storage.api.private.schemas import PrivateUploadResponse
import src.backend.storage.api.private.service as service

__all__ = [
    "router",
]

router = APIRouter(tags=["private"])


@router.post(
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
    return service.s3_batch_upload(
        minio_client=minio_client,
        files=files,
        user_id=user_id,
        chat_id=chat_id,
    )


@router.put(
    "/upload",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PrivateUploadResponse,
)
def upload(
    minio_client: minio_client_dependency,
    files: List[UploadFile] = File(...),
    file_ids: List[UUID] = Form(),
    user_id: UUID = Form(),
    chat_id: UUID = Form(),
):
    return service.s3_batch_upload(
        minio_client=minio_client,
        user_id=user_id,
        chat_id=chat_id,
        files=files,
        file_ids=file_ids,
    )
