from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    STORAGE_SERVICE_URL: str = "http://localhost:7000"
    SQL_DB_URL: str
    # Mount root shared with kafka-connect for filesystem sources (see
    # docker-compose.tmpl.yaml) — a resolved (symlink-followed) path must stay
    # under this root, or dedup.py's containment check rejects it.
    WATCH_ROOT: str = "/watch"


model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
