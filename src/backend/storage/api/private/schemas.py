from enum import Enum
from typing import List, Optional, Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator
from pydantic_core import from_json

_all_ = [
    "UserUploadRequest",
    "UserUploadResponse",
    "UserUploadUpdateRequest",
    "UserUploadUpdateResponse",
    "StreamUploadRequest",
    "StreamUploadResponse",
    "ObjectOperation",
]


class ObjectOperation(str, Enum):
    CREATE = "create"
    MODIFY = "modify"
    DELETE = "delete"


# -------------------------------- User Uploads ----------------------------------------
class UserUploadRequest(BaseModel):
    user_id: UUID = Field(
        title="User ID",
        description="""
            A unique identifier of the user.
            Will be used to form the bucket name""",
    )
    chat_id: UUID = Field(
        title="Chat Session ID",
        description="""
        A unique identifier for the chat ID.
        Will be used to form the bucket name
        """,
    )

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, data: Any) -> Any:
        if isinstance(data, str):
            return from_json(data)
        return data


class UserUploadResponse(BaseModel):
    file_ids: List[UUID] = Field(
        title="Documents IDs",
        description="""
        A list of documents stashed at the S3 bucket.
        """,
    )

    chat_id: UUID = Field(
        title="Chat Session ID",
        description="A unique identifier for the chat session.",
    )

    user_id: UUID = Field(
        title="User ID",
        description="A unique identifier of the user.",
    )

    task_ids: List[UUID] = Field(
        title="Task IDs",
        description="A list of unique identifiers for each upload task.",
    )


class UserUploadUpdateRequest(BaseModel):
    # List of document ID (UUID) that to be ingested and upserted to the vectorstore
    file_ids: List[UUID] = Field(
        title="Document IDs",
        description="List of UUID identifiers of updated documents",
    )

    # Chat ID - UUID format
    chat_id: UUID = Field(
        title="Chat Session ID",
        description="""
        A unique identifier for the chat ID.
        Will be used to form the bucket name
        """,
    )

    # a UUID for user_id
    user_id: UUID = Field(
        title="User ID",
        description="""
            A unique identifier of the user.
            Will be used to form the bucket name""",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, data: Any) -> Any:
        if isinstance(data, str):
            return from_json(data)
        return data


class UserUploadUpdateResponse(BaseModel):
    # List of document ID (UUID) that to be ingested and upserted to the vectorstore
    file_ids: List[UUID] = Field(
        title="Document IDs",
        description="List of successful UUID identifiers of upserted documents",
    )

    # Chat ID - UUID format
    chat_id: UUID = Field(
        title="Chat ID",
        description="UUID identifier of the chat to which the documents belong to",
    )

    user_id: UUID = Field(
        title="User ID",
        description="UUID identifier for the user",
    )

    task_ids: List[UUID] = Field(
        title="Task IDs",
        description="A list of unique identifiers for each upload task.",
    )


class UserUploadDeleteRequest(BaseModel):
    file_ids: List[UUID] = Field(
        title="Document IDs",
        description="""List of UUID identifiers of deleted documents.
        A comma separated list is expected.""",
        default_factory=list,
    )

    # a UUID for user_id
    user_id: UUID = Field(title="User ID", description="UUID identifier for the user")

    # Chat ID - UUID format
    # if not supplied, will be created on the fly
    # and thus create a new chat (not recommended)
    chat_id: UUID = Field(
        title="Chat ID",
        description="UUID identifier for the chat",
    )


# -------------------------------- User Uploads ----------------------------------------


# ------------------------------ Stream Uploads ----------------------------------------
# For request coming from Kafka
# Usually captured by a CDC process monitoring the system
class StreamUploadRequest(BaseModel):
    path: str = Field(
        title="File Path",
        description="Path to a file stored in the filesystem",
    )
    namespace: str = Field(
        title="Namespace",
        description="""The namespace to which the file belongs.
        Might be a project name, department, etc.""",
    )
    reason: str = Field(
        title="Event Reason",
        description="The reason for sending the file event - modified/deleted",
    )
    content_type: Optional[str] = Field(
        title="File Content Type",
        description="Optional Content-Type header",
        default=None,
    )


class StreamUploadResponse(BaseModel):
    bucket_name: str = Field(
        title="Bucket Name",
        description="The name of the bucket where the file is stashed",
    )
    object_name: str = Field(
        title="Object Name",
        description="The name of the S3 object to which the file is stashed to",
    )
