from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    STORAGE_SERVICE_URL: str = "http://localhost:7000"


model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
