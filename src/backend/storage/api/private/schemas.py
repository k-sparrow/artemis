from typing import List
from uuid import UUID

from pydantic import BaseModel


__all__ = [
    "PrivateUploadResponse",
]


class PrivateUploadResponse(BaseModel):
    user_id: UUID
    chat_id: UUID
    task_ids: List[UUID]
    file_ids: List[UUID]
