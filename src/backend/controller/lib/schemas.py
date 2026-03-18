# core
from enum import Enum
from uuid import UUID

# third party
from pydantic import (
    BaseModel,
    Field,
    Json,
)


class UploadAction(str, Enum):
    CREATE = "CREATE"
    UPDATE = "MODIFY"
    DELETE = "DELETE"
    AUTO_DELETE = "AUTO_DELETE"


class PrivateMetaInfo(BaseModel):
    user_id: UUID
    chat_id: UUID
    doc_id: UUID
    source: str


class LibraryMetaInfo(BaseModel):
    namespace: str
    source: str


InfoJson = Json[PrivateMetaInfo] | Json[LibraryMetaInfo]


class IngestRequest(BaseModel):
    """
    Request schema for ingesting an object into the system.

    Attributes:
        upload_action (UploadAction): The type of operation to perform
                                      (CREATE, UPDATE, DELETE).
        content_type (str): The MIME type of the object being ingested.
        bucket (str): The storage bucket where the object resides.
        object_ (str): The object key or path in the bucket.
                       Uses 'object' as the JSON field name.
        upload_type (UpsertionMetaInfoType): The type of upload (library or private).
        info (InfoJson): Additional metadata, either for private or library uploads.

    Example JSON payload:
    {
        "upload_type": "private",
        "upload_action": "CREATE",
        "content_type": "application/pdf",
        "bucket": "my-bucket",
        "object": "documents/example.pdf",
        "info": {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "chat_id": "223e4567-e89b-12d3-a456-426614174001",
            "doc_id": "323e4567-e89b-12d3-a456-426614174002"
        }
    }

    or for library:
    {
        "upload_type": "library",
        "upload_action": "CREATE",
        "content_type": "application/pdf",
        "bucket": "my-bucket",
        "object": "documents/example.pdf",
        "info": {
            "namespace": "Opentitan"
        }
    }
    """

    # operation type
    upload_action: UploadAction

    # object information
    content_type: str
    bucket: str
    object_: str = Field(alias="object")

    # upload type specific information
    info: InfoJson


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
    entire system.  Callers (Kafka HTTP Sink, tests, manual triggers) are
    responsible for resolving the correct UUID before dispatching the task —
    the worker no longer needs to know whether a document is "private" or
    "library".

    Migration note: replaces the old ``PrivateIngestionInfo`` /
    ``LibraryIngestionInfo`` split.  The Kafka Connect HTTP Sink config must be
    updated to send ``{"namespace_id": "<uuid>"}`` instead of the old payloads.
    """

    namespace_id: UUID
