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
    ingested_at: datetime

    model_config = {"from_attributes": True}


class GroupSummary(BaseModel):
    group_id: uuid.UUID
    object_count: int


class GroupDeleteResponse(BaseModel):
    task_ids: list[uuid.UUID] = Field(
        description="Tombstone task IDs dispatched, one per deleted object."
    )


class IngestionTaskResponse(BaseModel):
    """Built via ``router._to_task_response`` — never ``.model_validate()``
    directly off an ``ingestion_status`` row, since ``completed_at`` is
    derived (not a real column; see that helper's docstring)."""

    task_id: uuid.UUID
    obj_id: uuid.UUID | None
    namespace_id: uuid.UUID
    status: str = Field(description="'running', 'success', or 'failure'")
    stage: str = Field(description="Current/last pipeline task name, e.g. 'tasks.index'")
    operation: str
    failure_reason: str | None
    created_at: datetime
    completed_at: datetime | None = Field(
        description="Set once status is 'success' or 'failure'; None while running."
    )
