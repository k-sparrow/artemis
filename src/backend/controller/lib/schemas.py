# core
from enum import Enum
from uuid import UUID

# third party
from pydantic import BaseModel


class UploadAction(str, Enum):
    CREATE = "CREATE"
    UPDATE = "MODIFY"
    DELETE = "DELETE"
    AUTO_DELETE = "AUTO_DELETE"


class S3Details(BaseModel):
    bucket: str
    object: str


class SourceDetails(BaseModel):
    # File path might be null on DELETE operations
    # This does not matter as the canonical identification of
    # the object does not include the source path
    path: str | None
    content_type: str


class IngestionInfo(BaseModel):
    """Unified ingestion task parameter.

    The ``namespace_id`` is the single axis of data separation across the
    entire system.  Callers are responsible for resolving the correct UUID
    before dispatching the task.
    """

    namespace_id: UUID
