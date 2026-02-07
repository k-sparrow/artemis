from pydantic import computed_field
from pydantic_settings import BaseSettings

__all__ = [
    "IndexingSettings",
    "settings",
]


class IndexingSettings(BaseSettings):
    DEBUG: bool = False
    DOCLING_SERVE_URI: str
    QDRANT_HOST_URL: str
    QDRANT_COLLECTION_NAME: str
    TEI_HOST_URL: str

    @computed_field
    @property
    def QDRANT_HOST_URI(self) -> str:
        return f"{self.QDRANT_HOST_URL}"

    @computed_field
    @property
    def DOCLING_SERVE_HEALTHCHECL_URL(self) -> str:
        return f"{self.DOCLING_SERVE_URI.rstrip('/')}/health"


settings = IndexingSettings()
