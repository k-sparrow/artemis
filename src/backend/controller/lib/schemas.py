# core
from enum import Enum

# Re-export shared contract types so the worker can import them from here.
from src.lib.core.ingestion.contract import (  # noqa: F401
    BlobRef,
    FailureObject,
    FailureRecord,
    FailureScope,
    IngestionInfo,
    IngestionResult,
    IngestionTaskDetails,
    IndexingOutcome,
    ObjectMetadata,
    ObjectProperties,
    ObjectScope,
    S3Details,
    SourceDetails,
)


class UploadAction(str, Enum):
    CREATE = "CREATE"
    UPDATE = "MODIFY"
    DELETE = "DELETE"
    AUTO_DELETE = "AUTO_DELETE"
