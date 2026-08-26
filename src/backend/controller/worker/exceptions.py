"""Non-retryable and retryable failure types for the ingestion worker."""

import httpx

__all__ = [
    "EmptyObjectError",
    "S3IntegrityError",
    "ChunkIntegrityError",
    "QueueFullRetry",
]


class EmptyObjectError(Exception):
    """Raised when the S3 contract reports size=0 for a CREATE or UPDATE action.

    This is a permanent condition — retrying will not fill an empty file.
    """

    def __init__(self, obj_id: str) -> None:
        super().__init__(f"object {obj_id} was stored with 0 bytes — nothing to index")


class S3IntegrityError(Exception):
    """Raised when the contract reports size>0 but MinIO returned 0 bytes.

    Potentially transient (network blip during read); eligible for retry.
    """

    def __init__(self, s3_object: str, expected_size: int) -> None:
        super().__init__(
            f"MinIO integrity error: expected {expected_size} bytes for "
            f"{s3_object!r} but received 0"
        )


class ChunkIntegrityError(Exception):
    """Raised when the parsing service returns chunks with more than one obj_id.

    Indicates a bug in the parsing service contract. Non-retryable.
    """

    def __init__(self, obj_ids: set) -> None:
        super().__init__(
            f"parsing service returned chunks with multiple obj_ids: {obj_ids} — "
            "expected exactly one"
        )


class QueueFullRetry(httpx.HTTPStatusError):
    """docling-serve's Ray task queue is full — expected backpressure, not a
    failure. Same exception as the original 429, just with a __repr__ that
    matches this codebase's own structured log convention: passed as
    ``self.retry(exc=...)`` so Celery's built-in retry log line (which
    renders ``repr(exc)``) reads as a clean backpressure notice instead of
    dumping the raw httpx error text + MDN status-code link.
    """

    def __repr__(self) -> str:
        return "backpressure=docling_queue_full status=429"
