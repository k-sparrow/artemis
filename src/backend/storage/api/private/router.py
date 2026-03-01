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
from src.lib.backend.logging import get_logger

__all__ = [
    "router",
]

logger = get_logger("storage.private")

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
    logger.info(
        "upload_started",
        user_id=str(request.user_id),
        chat_id=str(request.chat_id),
        file_count=len(files),
    )
    try:
        result = service.s3_batch_upload(
            minio_client=minio_client,
            user_id=request.user_id,
            chat_id=request.chat_id,
            files=files,
        )
        logger.info(
            "upload_completed",
            user_id=str(request.user_id),
            chat_id=str(request.chat_id),
            file_ids=result["file_ids"],
        )
        return result
    except Exception as e:
        logger.error(
            "upload_failed",
            user_id=str(request.user_id),
            chat_id=str(request.chat_id),
            error=str(e),
        )
        raise


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
    logger.info(
        "update_started",
        user_id=str(request.user_id),
        chat_id=str(request.chat_id),
        file_count=len(files),
    )
    try:
        result = service.s3_batch_upload(
            minio_client=minio_client,
            user_id=request.user_id,
            chat_id=request.chat_id,
            files=files,
            file_ids=request.file_ids,
        )
        logger.info(
            "update_completed",
            user_id=str(request.user_id),
            chat_id=str(request.chat_id),
            file_ids=result["file_ids"],
        )
        return result
    except Exception as e:
        logger.error(
            "update_failed",
            user_id=str(request.user_id),
            chat_id=str(request.chat_id),
            error=str(e),
        )
        raise


@router.delete(
    "/files",
    status_code=status.HTTP_204_NO_CONTENT,
)
def private_delete(
    minio_client: minio_client_dependency,
    request: UserUploadDeleteRequest,
):
    logger.info(
        "delete_started",
        user_id=str(request.user_id),
        chat_id=str(request.chat_id),
        file_count=len(request.file_ids),
    )
    try:
        service.s3_batch_delete(
            minio_client=minio_client,
            user_id=request.user_id,
            chat_id=request.chat_id,
            file_ids=request.file_ids,
        )
        logger.info(
            "delete_completed",
            user_id=str(request.user_id),
            chat_id=str(request.chat_id),
        )
    except Exception as e:
        logger.error(
            "delete_failed",
            user_id=str(request.user_id),
            chat_id=str(request.chat_id),
            error=str(e),
        )
        raise
