from pydantic import computed_field
from pydantic_settings import BaseSettings

from src.lib.core.ingestion.config import PipelineType

__all__ = [
    "IndexingSettings",
    "settings",
]


class IndexingSettings(BaseSettings):
    """Infrastructure settings — connection strings, service addresses, and
    operator-level defaults for algorithm parameters.

    Algorithm choices (pipeline type, chunk sizes) are request-level
    parameters that can be overridden per-request, but their defaults are
    configurable here so that a deployment can be tuned without code changes.
    """

    DEBUG: bool = False
    # Pipeline algorithm defaults (overridable per-request via query params)
    DEFAULT_PIPELINE_TYPE: PipelineType = PipelineType.SIMPLE
    DEFAULT_CHUNK_SIZE: int = 1024
    DEFAULT_CHUNK_OVERLAP: int = 100
    # External service URLs
    DOCLING_SERVE_URI: str
    QDRANT_HOST_URL: str
    QDRANT_COLLECTION_NAME: str
    TEI_HOST_URL: str
    # SQL / Postgres
    SQL_DB_HOST: str
    SQL_DB_PORT: int
    SQL_DB_USER: str
    SQL_DB_PASSWORD: str
    SQL_DB_DATABASE: str
    SQL_DRIVER: str = "postgresql+asyncpg"

    @computed_field
    @property
    def SQL_DB_URL(self) -> str:
        return f"{self.SQL_DRIVER}://{self.SQL_DB_USER}:{self.SQL_DB_PASSWORD}@{self.SQL_DB_HOST}:{self.SQL_DB_PORT}/{self.SQL_DB_DATABASE}"

    @computed_field
    @property
    def QDRANT_HOST_URI(self) -> str:
        return f"{self.QDRANT_HOST_URL}"

    @computed_field
    @property
    def DOCLING_SERVE_HEALTHCHECL_URL(self) -> str:
        return f"{self.DOCLING_SERVE_URI.rstrip('/')}/health"


settings = IndexingSettings()
