from pydantic_settings import BaseSettings

__all__ = [
    "McpSettings",
    "settings",
]


class McpSettings(BaseSettings):
    STORAGE_SERVICE_URL: str
    INDEXING_SERVICE_URL: str
    STUB_OWNER_ID: str = "00000000-0000-0000-0000-000000000000"
    DEBUG: bool = False


settings = McpSettings()
