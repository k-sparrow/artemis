from __future__ import annotations

import uuid
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class FilesystemSource(BaseModel):
    type: Literal["filesystem"]
    path: str = Field(description="Absolute path to the file on the mounted volume.")


class InlineSource(BaseModel):
    type: Literal["inline"]
    content: str = Field(
        description="Text content of the document (PR body, comment, etc.)."
    )
    encoding: str = Field(
        default="utf-8", description="Text encoding for byte conversion."
    )


class UrlSource(BaseModel):
    type: Literal["url"]
    url: str = Field(
        description="URL to fetch. Enterprise intake service fetches the bytes."
    )


Source = Annotated[
    Union[FilesystemSource, InlineSource, UrlSource],
    Field(discriminator="type"),
]


class IntakeRequest(BaseModel):
    source: Source
    display_name: str = Field(
        description="Display name for the document (e.g. 'spec.pdf', 'pr-123.md')."
    )
    namespace_id: uuid.UUID = Field(
        description="Namespace UUID issued by the storage service."
    )
    group_id: uuid.UUID | None = Field(
        default=None,
        description="Logical group owner (connector_id for enterprise sources).",
    )
    owner_id: uuid.UUID = Field(
        description=(
            "Namespace owner UUID, " "forwarded as X-Owner-Id to the storage service."
        ),
    )


class IntakeResponse(BaseModel):
    task_id: uuid.UUID
    namespace_id: uuid.UUID
