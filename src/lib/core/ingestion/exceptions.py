__all__ = [
    "DocumentProcessingException",
]


class DocumentProcessingException(Exception):
    """Raised when document processing fails in the ingestion pipeline."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
