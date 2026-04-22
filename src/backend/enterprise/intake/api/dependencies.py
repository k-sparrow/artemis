from typing import Annotated

import httpx
from fastapi import Depends

from src.backend.enterprise.intake.api.config import settings

storage_client: httpx.AsyncClient = httpx.AsyncClient(
    base_url=settings.STORAGE_SERVICE_URL
)


async def get_storage_client() -> httpx.AsyncClient:
    return storage_client


storage_client_dependency = Annotated[httpx.AsyncClient, Depends(get_storage_client)]
