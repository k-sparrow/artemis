from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "WorkerSettings",
    "settings",
]


class WorkerSettings(BaseSettings):
    """Configuration settings for the Artemis ingestion controller worker."""

    model_config = SettingsConfigDict(extra="ignore")

    # MinIO S3 client configuration
    S3_ENDPOINT: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str
    S3_SECURE: bool

    # RabbitMQ broker
    RABBITMQ_USER: str
    RABBITMQ_PASSWORD: str
    RABBITMQ_HOST: str
    RABBITMQ_PORT: int
    RABBITMQ_VHOST: str

    # Postgres result backend
    SQL_DB_HOST: str
    SQL_DB_PORT: int
    SQL_DB_USER: str
    SQL_DB_PASSWORD: str
    SQL_DB_DATABASE: str
    SQL_DRIVER: str

    @computed_field
    def MESSAGE_BROKER_URL(self) -> str:
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/{self.RABBITMQ_VHOST}"  # noqa: E501

    @computed_field
    def BACKEND_RESULT_URL(self) -> str:
        return f"{self.SQL_DRIVER}://{self.SQL_DB_USER}:{self.SQL_DB_PASSWORD}@{self.SQL_DB_HOST}:{self.SQL_DB_PORT}/{self.SQL_DB_DATABASE}"  # noqa: E501

    # Upstream service URLs
    PARSING_SERVICE_URL: str  # e.g. http://backend-parsing:10001
    INGESTION_SERVICE_URL: str  # e.g. http://backend-indexing:10000

    # MinIO bucket for parsed-chunk intermediate objects
    PARSED_CHUNKS_BUCKET: str = "parsed-chunks"

    # httpx timeout (seconds) for legacy parsing and indexing service calls
    HTTPX_TIMEOUT: float = 86400.0

    # Per-call timeouts for the new async parse endpoints
    PARSING_SUBMIT_TIMEOUT: float = 120.0  # shard uploads + HTTP POST
    PARSING_STATUS_TIMEOUT: float = 30.0  # single status GET
    PARSING_RESOLVE_TIMEOUT: float = 120.0  # S3 downloads + concat + chunk submit
    PARSING_FINALIZE_TIMEOUT: float = 60.0  # chunk result fetch + artifact write

    # Celery exchange name
    EXCHANGE_NAME: str

    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""


settings = WorkerSettings()  # type: ignore
