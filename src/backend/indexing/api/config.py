from pydantic import computed_field
from pydantic_settings import BaseSettings

from src.lib.core.adapters.vectorstore.qdrant import RetrievalMode
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
    RETRIEVAL_MODE: RetrievalMode = RetrievalMode.DENSE
    # ColBERT late-interaction — required when RETRIEVAL_MODE=multi_stage
    COLBERT_HOST_URL: str | None = None
    COLBERT_MODEL_NAME: str = "colbert-ir/colbertv2.0"
    # Pipeline algorithm defaults (overridable per-request via query params)
    DEFAULT_PIPELINE_TYPE: PipelineType = PipelineType.SIMPLE
    DEFAULT_CHUNK_SIZE: int = 1024
    DEFAULT_CHUNK_OVERLAP: int = 100
    # ColBERT reranker — optional; enables ContextualCompressionRetriever on /retrieve
    COLBERT_RERANKER_URL: str | None = None
    RETRIEVE_CANDIDATES_MULTIPLIER: int = 10
    # External service URLs
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
        return f"{self.SQL_DRIVER}://{self.SQL_DB_USER}:{self.SQL_DB_PASSWORD}@{self.SQL_DB_HOST}:{self.SQL_DB_PORT}/{self.SQL_DB_DATABASE}"  # noqa: E501

    @computed_field
    @property
    def QDRANT_HOST_URI(self) -> str:
        return f"{self.QDRANT_HOST_URL}"


settings = IndexingSettings()
