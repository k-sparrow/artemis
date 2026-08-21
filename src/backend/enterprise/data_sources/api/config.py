from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    SQL_DB_URL: str
    STORAGE_SERVICE_URL: str = "http://localhost:7000"
    KAFKA_CONNECT_URL: str = "http://localhost:8083"
    # Production-safe defaults for the FileSource connector template (see
    # templates.py's own module constants for the full rationale). Overridable
    # per-environment so dev/test can get fast feedback without touching the
    # production default: see docker-compose.tmpl.yaml's dev-only override.
    FILESOURCE_IDEMPOTENT_CACHE_SIZE: int = 50_000
    FILESOURCE_POLL_DELAY_MS: int = 600_000

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
