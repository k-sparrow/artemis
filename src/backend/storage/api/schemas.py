from typing import List
from uuid import UUID

from fastapi import Form, UploadFile, File
from pydantic import BaseModel, Field


__all__ = [
    "PrivateUploadResponse",
]


class PrivateUploadResponse(BaseModel):
    user_id: UUID
    chat_id: UUID
    task_ids: List[UUID]
    file_ids: List[UUID]
