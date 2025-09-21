from uuid import UUID
from typing import List

from fastapi import APIRouter, status, UploadFile, File, Form

from src.backend.storage.api.dependencies import minio_client_dependency
from src.backend.storage.api.private.schemas import (
    UserUploadRequest,
    UserUploadResponse,
    UserUploadUpdateRequest,
    UserUploadUpdateResponse,
    UserUploadDeleteRequest,
)
import src.backend.storage.api.private.service as service

__all__ = [
    "router",
]

router = APIRouter(tags=["User"])


@router.post(
    "/files",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=UserUploadResponse,
)
def private_upload(
    minio_client: minio_client_dependency,
    request: UserUploadRequest = Form(...),
    files: List[UploadFile] = File(...),
):
    return service.s3_batch_upload(
        minio_client=minio_client,
        user_id=request.user_id,
        chat_id=request.chat_id,
        files=files,
    )


@router.put(
    "/files",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=UserUploadUpdateResponse,
)
def private_update(
    minio_client: minio_client_dependency,
    request: UserUploadUpdateRequest = Form(...),
    files: List[UploadFile] = File(...),
):
    return service.s3_batch_upload(
        minio_client=minio_client,
        user_id=request.user_id,
        chat_id=request.chat_id,
        files=files,
        file_ids=request.file_ids,
    )


@router.delete(
    "/files",
    status_code=status.HTTP_204_NO_CONTENT,
)
def private_delete(
    minio_client: minio_client_dependency,
    request: UserUploadDeleteRequest,
):
    return service.s3_batch_delete(
        minio_client=minio_client,
        user_id=request.user_id,
        chat_id=request.chat_id,
        file_ids=request.file_ids,
    )
