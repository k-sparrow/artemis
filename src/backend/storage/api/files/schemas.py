"""Pydantic request / response schemas for the /objects and /tasks routes."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class ObjectUploadResponse(BaseModel):
    task_id: uuid.UUID = Field(
        description="Pre-generated Celery task ID to poll against."
    )


class IngestedObjectResponse(BaseModel):
    id: uuid.UUID
    namespace_id: uuid.UUID
    source: str
    object_type: str
    content_type: str
    size_bytes: int | None
    group_id: uuid.UUID | None

    model_config = {"from_attributes": True}


class GroupDeleteResponse(BaseModel):
    task_ids: list[uuid.UUID] = Field(
        description="Tombstone task IDs dispatched, one per deleted object."
    )


class IngestionTaskResponse(BaseModel):
    task_id: uuid.UUID
    obj_id: uuid.UUID | None
    namespace_id: uuid.UUID
    status: str
    failure_reason: str | None
    completed_at: datetime

    model_config = {"from_attributes": True}
