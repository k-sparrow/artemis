from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    SQL_DB_URL: str
    STORAGE_SERVICE_URL: str = "http://localhost:7000"
    KAFKA_CONNECT_URL: str = "http://localhost:8083"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
