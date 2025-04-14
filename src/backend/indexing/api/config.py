from pydantic_settings import BaseSettings

__all__ = [
    "IndexingSettings",
    "settings",
]


class IndexingSettings(BaseSettings):
    DOCLING_BACKEND_URL: str
    QDRANT_HOST_URL: str
    QDRANT_HOST_PORT: str
    QDRANT_COLLECTION_NAME: str
    TEI_HOST_URL: str


settings = IndexingSettings()
