from pydantic_settings import BaseSettings

__all__ = [
    "IndexingSettings",
    "settings",
]


class IndexingSettings(BaseSettings):
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str


settings = IndexingSettings()
