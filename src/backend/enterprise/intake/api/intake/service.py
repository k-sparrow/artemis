"""Business logic for the enterprise intake endpoint.

Flow:
  1. Verify namespace_id exists on the storage service (GET /namespaces/{id}).
  2. Materialise bytes from the source:
       filesystem — read from mounted FS path
       inline     — encode text content to bytes
       url        — fetch over HTTP
  3. POST bytes to the storage service upload endpoint.

The namespace UUID is always provided by the caller — data_sources owns
namespace creation and injects the UUID into the Kafka message headers.
This service has zero knowledge of MinIO, Kafka, Qdrant, or Celery internals.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import httpx
from fastapi import status

from src.backend.enterprise.intake.api.intake.exceptions import (
    NamespaceNotFoundError,
    PathNotFoundError,
    StorageServiceError,
    UrlFetchError,
)
from src.backend.enterprise.intake.api.intake.schemas import (
    FilesystemSource,
    InlineSource,
    IntakeRequest,
    UrlSource,
)


async def _verify_namespace(http: httpx.AsyncClient, namespace_id: uuid.UUID) -> None:
    """Verify the namespace exists on the storage service.

    Raises StorageServiceError (→ 422) if the namespace is not found.
    """
    resp = await http.get(f"/namespaces/{namespace_id}")
    if resp.status_code == status.HTTP_404_NOT_FOUND:
        raise NamespaceNotFoundError(str(namespace_id))
    if resp.status_code != status.HTTP_200_OK:
        raise StorageServiceError(resp.status_code, resp.text)


async def _resolve_bytes(source: FilesystemSource | InlineSource | UrlSource) -> bytes:
    """Materialise the document bytes from whichever source type was provided."""
    if isinstance(source, FilesystemSource):
        path = Path(source.path)
        if not path.exists():
            raise PathNotFoundError(source.path)
        return path.read_bytes()

    if isinstance(source, InlineSource):
        return source.content.encode(source.encoding)

    # UrlSource — fetch over HTTP.
    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
        try:
            resp = await client.get(source.url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise UrlFetchError(source.url, exc.response.status_code) from exc
        except httpx.RequestError as exc:
            raise UrlFetchError(source.url, None) from exc
    return resp.content


async def intake_file(
    http: httpx.AsyncClient,
    request: IntakeRequest,
) -> tuple[uuid.UUID, str]:
    """Verify namespace → resolve bytes → upload to storage service.

    Returns ``(task_id, s3_key)``.
    """
    # 1. Verify the namespace exists.
    await _verify_namespace(http, request.namespace_id)

    # 2. Materialise bytes from the source.
    data = await _resolve_bytes(request.source)

    # 3. Upload to the storage service.
    upload_resp = await http.post(
        f"/namespaces/{request.namespace_id}/objects",
        files={"file": (request.display_name, data, request.content_type)},
    )
    if upload_resp.status_code != status.HTTP_202_ACCEPTED:
        raise StorageServiceError(upload_resp.status_code, upload_resp.text)

    body = upload_resp.json()
    return uuid.UUID(body["task_id"]), body["s3_key"]
