from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    STORAGE_SERVICE_URL: str = "http://localhost:7000"
    SQL_DB_URL: str
    # Mount roots shared with kafka-connect for filesystem sources (see
    # docker-compose.tmpl.yaml) — a resolved (symlink-followed) path must stay
    # under one of these roots, or service.py's containment check rejects it.
    # Deployments watching more than one directory (e.g. separate NFS mounts)
    # must list every one here — an unlisted root's files get rejected with
    # PathEscapesWatchRootError, not silently ignored. Env var accepts a JSON
    # array, e.g. WATCH_ROOTS=["/watch","/proj/eng/ot2/my_html/Apollo_rag_files"].
    WATCH_ROOTS: list[str] = ["/watch"]


model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
