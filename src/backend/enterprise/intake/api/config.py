from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    STORAGE_SERVICE_URL: str = "http://localhost:7000"
    KAFKA_CONNECT_URL: str = "http://localhost:8083"
    HTTP_SINK_CONNECTOR_NAME: str = "artemis-enterprise-http-sink"
    INTAKE_URL: str

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
