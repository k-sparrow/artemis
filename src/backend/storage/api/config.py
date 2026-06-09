from pydantic_settings import BaseSettings

__all__ = [
    "StorageSettings",
    "settings",
]


class StorageSettings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""

    # S3 configuration
    S3_ENDPOINT_URL: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str

    # Bucket settings
    S3_ARTEMIS_BUCKET: str
    S3_ARTEMIS_BUCKET_KAFKA_EVENT: str

    # Postgres (asyncpg DSN)
    SQL_DB_URL: str


settings = StorageSettings()
