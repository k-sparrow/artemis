from pydantic import computed_field
from pydantic_settings import BaseSettings

__all__ = [
    "ParsingSettings",
    "settings",
]


class ParsingSettings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    DOCLING_SERVE_URI: str

    # MinIO S3 client — used to read input by reference and write parse
    # artifacts (claim-check). Required (no defaults) so a misconfigured
    # deployment fails fast at startup rather than silently building an
    # unreachable client.
    S3_ENDPOINT: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str
    S3_SECURE: bool

    # Bucket where parse artifacts are written for the indexing service to read.
    PARSED_ARTIFACTS_BUCKET: str = "parsed-chunks"

    # Private bucket for the lossless DoclingDocument replay cache. Never part of
    # the parse→index contract — insurance for re-chunk / future citations
    # without a GPU re-parse.
    REPLAY_CACHE_BUCKET: str = "docling-replay"

    # Per-call HTTP timeouts for the new async parse endpoints.
    DOCLING_STATUS_TIMEOUT: float = 30.0
    DOCLING_RESOLVE_TIMEOUT: float = 120.0
    DOCLING_FINALIZE_TIMEOUT: float = 60.0

    # This service's own externally-reachable address — the same host:port the
    # worker already reaches it at via its own PARSING_SERVICE_URL. Used to
    # build the callback URL docling-serve POSTs the terminal conversion
    # result back to (POST {PARSING_SERVICE_PUBLIC_URL}/v1/parse/callback/{obj_id}).
    PARSING_SERVICE_PUBLIC_URL: str

    # RabbitMQ broker — this service only ever PUBLISHES (send_task by name,
    # never consumes), so it needs no result backend or worker config, just
    # the same broker/exchange the controller worker already uses.
    RABBITMQ_USER: str
    RABBITMQ_PASSWORD: str
    RABBITMQ_HOST: str
    RABBITMQ_PORT: int
    RABBITMQ_VHOST: str
    EXCHANGE_NAME: str

    @computed_field
    @property
    def DOCLING_SERVE_HEALTHCHECK_URL(self) -> str:
        return f"{self.DOCLING_SERVE_URI.rstrip('/')}/health"

    @computed_field
    @property
    def MESSAGE_BROKER_URL(self) -> str:
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/{self.RABBITMQ_VHOST}"  # noqa: E501


settings = ParsingSettings()
