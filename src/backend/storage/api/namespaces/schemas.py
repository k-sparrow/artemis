"""Pydantic request / response schemas for the /namespaces routes."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field, model_validator

from src.backend.storage.api.models import NamespaceType


class NamespaceCreate(BaseModel):
    type: NamespaceType
    name: str | None = Field(
        default=None,
        description=(
            "Display name. Required for SHARED (used as UUID5 seed). "
            "Optional for PRIVATE — auto-generated from timestamp if omitted."
        ),
    )

    @model_validator(mode="after")
    def shared_requires_name(self) -> "NamespaceCreate":
        if self.type == NamespaceType.SHARED and not self.name:
            raise ValueError("SHARED namespaces require a name")
        return self


class NamespaceRename(BaseModel):
    name: str = Field(description="New display name. PRIVATE namespaces only.")


class NamespaceResponse(BaseModel):
    id: uuid.UUID
    name: str
    type: NamespaceType
    owner_id: uuid.UUID
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
