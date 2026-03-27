"""Pydantic request / response schemas for the /files routes."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class FileUploadResponse(BaseModel):
    task_id: uuid.UUID = Field(
        description="Pre-generated Celery task ID to poll against."
    )
    s3_key: str = Field(description="MinIO object key where the file was stored.")


class IngestedFileResponse(BaseModel):
    id: uuid.UUID
    namespace_id: uuid.UUID
    task_id: uuid.UUID
    filename: str
    content_type: str
    size_bytes: int | None
    status: str
    failure_reason: str | None
    completed_at: datetime

    model_config = {"from_attributes": True}


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: str | None
    traceback: str | None
    date_done: datetime | None
