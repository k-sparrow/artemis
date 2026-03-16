from pydantic import computed_field
from pydantic_settings import BaseSettings

from src.lib.core.adapters.loaders import LoaderType

__all__ = [
    "ParsingSettings",
    "settings",
]


class ParsingSettings(BaseSettings):
    DEBUG: bool = False
    DOCLING_SERVE_URI: str
    LOADER_TYPE: LoaderType = LoaderType.DOCLING

    @computed_field
    @property
    def DOCLING_SERVE_HEALTHCHECK_URL(self) -> str:
        return f"{self.DOCLING_SERVE_URI.rstrip('/')}/health"


settings = ParsingSettings()
