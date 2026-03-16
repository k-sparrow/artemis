# third party
from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "WorkerSettings",
    "settings",
]


class WorkerSettings(BaseSettings):
    """
    Configuration settings for the Apollo Backend Ingestion Controller API.
    """

    model_config = SettingsConfigDict(extra="ignore")

    # MinIO S3 client configuration
    # Note: These values can be overridden by environment variables.
    # DO NOT use these hard coded values in production
    S3_ENDPOINT: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str
    S3_SECURE: bool

    # credentials
    RABBITMQ_USER: str
    RABBITMQ_PASSWORD: str
    RABBITMQ_HOST: str
    RABBITMQ_PORT: int
    RABBITMQ_VHOST: str

    # Result backend URL
    SQL_DB_HOST: str
    SQL_DB_PORT: int
    SQL_DB_USER: str
    SQL_DB_PASSWORD: str
    SQL_DB_DATABASE: str
    SQL_DRIVER: str

    @computed_field
    def MESSAGE_BROKER_URL(self) -> str:
        """Construct the RabbitMQ message broker URL."""
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/{self.RABBITMQ_VHOST}"  # noqa: E501

    @computed_field
    def BACKEND_RESULT_URL(self) -> str:
        """Construct the SQL backend result URL."""
        return f"{self.SQL_DRIVER}://{self.SQL_DB_USER}:{self.SQL_DB_PASSWORD}@{self.SQL_DB_HOST}:{self.SQL_DB_PORT}/{self.SQL_DB_DATABASE}"  # noqa: E501

    # Task settings
    EXCHANGE_NAME: str

    # Ingestion service URLs
    INGESTION_SERVICE_URL: str
    INGESTION_SERVICE_URL: str


settings = WorkerSettings()  # type: ignore
