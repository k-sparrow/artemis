"""Async HTTP client for the Artemis data sources API (via APISIX gateway)."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

import httpx

from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceCreate, DataSourceResponse
from src.cli.settings import settings


class DataSourcesClient:
    """Thin async wrapper around GET/POST/DELETE /data-sources/* on the gateway."""

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = base_url or settings.GATEWAY_URL

    @asynccontextmanager
    async def _session(self):
        async with httpx.AsyncClient(base_url=self._base_url, timeout=30.0) as client:
            yield client

    async def list_sources(self) -> list[DataSourceResponse]:
        async with self._session() as client:
            resp = await client.get("/data-sources")
            resp.raise_for_status()
            return [DataSourceResponse.model_validate(item) for item in resp.json()]

    async def get_source(self, source_id: uuid.UUID) -> DataSourceResponse:
        async with self._session() as client:
            resp = await client.get(f"/data-sources/{source_id}")
            resp.raise_for_status()
            return DataSourceResponse.model_validate(resp.json())

    async def create_source(self, payload: DataSourceCreate) -> DataSourceResponse:
        async with self._session() as client:
            resp = await client.post(
                "/data-sources",
                json=payload.model_dump(mode="json"),
            )
            resp.raise_for_status()
            return DataSourceResponse.model_validate(resp.json())

    async def delete_source(self, source_id: uuid.UUID) -> None:
        async with self._session() as client:
            resp = await client.delete(f"/data-sources/{source_id}")
            resp.raise_for_status()

    async def pause_source(self, source_id: uuid.UUID) -> DataSourceResponse:
        async with self._session() as client:
            resp = await client.post(f"/data-sources/{source_id}/pause")
            resp.raise_for_status()
            return DataSourceResponse.model_validate(resp.json())

    async def resume_source(self, source_id: uuid.UUID) -> DataSourceResponse:
        async with self._session() as client:
            resp = await client.post(f"/data-sources/{source_id}/resume")
            resp.raise_for_status()
            return DataSourceResponse.model_validate(resp.json())

    async def restart_source(self, source_id: uuid.UUID) -> DataSourceResponse:
        async with self._session() as client:
            resp = await client.post(f"/data-sources/{source_id}/restart")
            resp.raise_for_status()
            return DataSourceResponse.model_validate(resp.json())
