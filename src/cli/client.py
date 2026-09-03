"""Async HTTP clients for the Artemis API (via APISIX gateway)."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

import httpx

from src.backend.enterprise.data_sources.api.sources.schemas import (
    DataSourceCreate,
    DataSourceResponse,
)
from src.backend.storage.api.files.schemas import (
    IngestedObjectResponse,
    IngestionTaskResponse,
)
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

    async def delete_namespace_sources(self, namespace_id: uuid.UUID) -> None:
        async with self._session() as client:
            resp = await client.delete(f"/data-sources/namespace/{namespace_id}")
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


class StorageClient:
    """Thin async wrapper around the storage service (via APISIX gateway)."""

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = base_url or settings.GATEWAY_URL

    @asynccontextmanager
    async def _session(self, owner_id: uuid.UUID):
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=30.0,
            headers={"X-Owner-Id": str(owner_id)},
        ) as client:
            yield client

    async def list_objects(
        self,
        namespace_id: uuid.UUID,
        owner_id: uuid.UUID,
        limit: int = 10,
        order: str = "desc",
        group_id: uuid.UUID | None = None,
    ) -> list[IngestedObjectResponse]:
        params: dict = {"limit": limit, "order": order}
        if group_id is not None:
            params["group_id"] = str(group_id)
        async with self._session(owner_id) as client:
            resp = await client.get(
                f"/namespaces/{namespace_id}/objects",
                params=params,
            )
            resp.raise_for_status()
            return [IngestedObjectResponse.model_validate(o) for o in resp.json()]

    async def list_tasks(
        self,
        namespace_id: uuid.UUID,
        owner_id: uuid.UUID,
        limit: int | None = None,
        order: str = "desc",
    ) -> list[IngestionTaskResponse]:
        params: dict = {"order": order}
        if limit is not None:
            params["limit"] = limit
        async with self._session(owner_id) as client:
            resp = await client.get(
                f"/namespaces/{namespace_id}/tasks", params=params
            )
            resp.raise_for_status()
            return [IngestionTaskResponse.model_validate(t) for t in resp.json()]

    async def delete_object(
        self,
        namespace_id: uuid.UUID,
        obj_id: uuid.UUID,
        owner_id: uuid.UUID,
    ) -> None:
        async with self._session(owner_id) as client:
            resp = await client.delete(f"/namespaces/{namespace_id}/objects/{obj_id}")
            resp.raise_for_status()
