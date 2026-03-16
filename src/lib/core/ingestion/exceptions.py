__all__ = [
    "DocumentProcessingException",
    "UpstreamServiceException",
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
