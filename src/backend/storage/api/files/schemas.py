"""Pydantic request / response schemas for the /objects routes."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class ObjectUploadResponse(BaseModel):
    task_id: uuid.UUID = Field(
        description="Pre-generated Celery task ID to poll against."
    )
    s3_key: str = Field(description="MinIO object key where the object was stored.")


class IngestedObjectResponse(BaseModel):
    id: uuid.UUID
    namespace_id: uuid.UUID
    task_id: uuid.UUID
    source: str
    object_type: str
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
