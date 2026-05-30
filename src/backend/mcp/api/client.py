import httpx

from src.backend.mcp.api.settings import settings

__all__ = [
    "storage_client",
    "indexing_client",
]

storage_client: httpx.AsyncClient = httpx.AsyncClient(
    base_url=settings.STORAGE_SERVICE_URL,
    timeout=30.0,
)

indexing_client: httpx.AsyncClient = httpx.AsyncClient(
    base_url=settings.INDEXING_SERVICE_URL,
    timeout=30.0,
)
