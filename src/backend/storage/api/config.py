from pydantic_settings import BaseSettings

__all__ = [
    "StorageSettings",
    "settings",
]


class StorageSettings(BaseSettings):
    DEBUG: bool = False

    # S3 configuration
    S3_ENDPOINT_URL: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str

    # Bucket settings
    S3_VENUS_BUCKET: str
    S3_VENUS_BUCKET_KAFKA_EVENT: str


settings = StorageSettings()
