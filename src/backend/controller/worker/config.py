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

    # httpx timeout (seconds) for indexing and delete service calls. Like every
    # timeout below, RabbitMQ's consumer_timeout must exceed twice the longest
    # call on a worker plus its longest retry countdown — a countdown retry
    # stays unacked while it waits for a busy slot (compose rabbitmq-conf).
    HTTPX_TIMEOUT: float = 1800.0

    # Per-call timeouts for the async parse/chunk endpoints. Each must stay
    # ABOVE the parsing service's matching DOCLING_*_TIMEOUT
    # (120/30/120/1200/120s) — otherwise this side gives up first and sees a
    # bare read timeout instead of the parsing service's classified 503. Only
    # chunk submit is sized for very large documents: it is the one call where
    # the parsing service still moves the whole document (an inline upload to
    # docling-serve), which is why submit_chunk has its own one-slot queue.
    PARSING_SUBMIT_TIMEOUT: float = 180.0  # source download/forward + HTTP POST
    PARSING_STATUS_TIMEOUT: float = 45.0  # single status GET (parse or chunk task)
    PARSING_RESOLVE_TIMEOUT: float = (
        300.0  # S3 server-side copy of the result + pages cache
    )
    PARSING_CHUNK_SUBMIT_TIMEOUT: float = (
        1500.0  # replay-cache read + inline upload to docling-serve
    )
    PARSING_FINALIZE_TIMEOUT: float = 180.0  # chunk result fetch + artifact write

    # Celery exchange name
    EXCHANGE_NAME: str

    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""


settings = WorkerSettings()  # type: ignore
