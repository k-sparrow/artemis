from pydantic import computed_field
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

    @computed_field
    @property
    def QDRANT_HOST_URI(self) -> str:
        return f"{self.QDRANT_HOST_URL}:{self.QDRANT_HOST_PORT}"


settings = IndexingSettings()
