__all__ = [
    "DocumentProcessingException",
    "UpstreamServiceException",
    "UpstreamBackpressureException",
]


class DocumentProcessingException(Exception):
    """Raised when document processing fails in the ingestion pipeline."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class UpstreamServiceException(Exception):
    """
    Raised when a required upstream service
    (loader, embeddings, vectorstore) is unavailable.
    """

    def __init__(self, service: str, message: str):
        self.service = service
        self.message = message
        super().__init__(f"{service}: {message}")


class UpstreamBackpressureException(UpstreamServiceException):
    """Raised when an upstream service (docling-serve) actively rejects a
    request because it's at capacity (429), as opposed to being unreachable
    or erroring (UpstreamServiceException/503). Distinct from its parent so
    callers can map it to 429 specifically -- the worker's retry policy
    (src/backend/controller/worker/tasks.py) treats 429 differently from
    5xx: flat delay with a 24h budget, not exponential backoff, and it's
    excluded from the circuit breaker's failure count. A 429 that gets
    collapsed into the parent's 503 mapping would still retry, but with the
    wrong cadence and would wrongly count toward the breaker."""
