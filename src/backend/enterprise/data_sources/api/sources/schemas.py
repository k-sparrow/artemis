"""Pydantic request/response schemas for the /data-sources routes."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DataSourceCreate(BaseModel):
    display_name: str = Field(description="Human-readable label for this data source.")
    path: str = Field(description="Absolute filesystem path to watch.")
    namespace: str = Field(
        description="Artemis namespace name (UUID5 seed for SHARED namespace)."
    )
    org_name: str = Field(description="Organisation name (UUID5 seed for owner_id).")
    recursive: bool = Field(
        default=True,
        description="Watch subdirectories recursively.",
    )


class ConnectorTaskStatus(BaseModel):
    id: int
    state: str
    worker_id: str
    trace: str | None = None


class KafkaConnectStatus(BaseModel):
    state: str
    worker_id: str
    tasks: list[ConnectorTaskStatus] = []


class DataSourceResponse(BaseModel):
    id: uuid.UUID
    display_name: str
    source_type: str
    connector_name: str
    namespace_id: uuid.UUID
    org_name: str
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    kafka_status: KafkaConnectStatus | None = None

    model_config = {"from_attributes": True}
