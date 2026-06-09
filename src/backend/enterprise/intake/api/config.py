from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    STORAGE_SERVICE_URL: str = "http://localhost:7000"


model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
