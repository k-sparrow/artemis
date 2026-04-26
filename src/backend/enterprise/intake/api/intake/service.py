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
import mimetypes

import filetype
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


def _infer_mime(data: bytes, name: str = "") -> str:
    """Infer MIME type from file bytes, falling back to filename extension."""
    kind = filetype.guess(data)
    if kind is not None:
        return kind.mime
    guessed, _ = mimetypes.guess_type(name)
    return guessed or "application/octet-stream"


async def _resolve_bytes(
    source: FilesystemSource | InlineSource | UrlSource,
) -> tuple[bytes, str]:
    """Materialise the document bytes and infer MIME type from the source.

    Returns ``(data, content_type)``.
    """
    if isinstance(source, FilesystemSource):
        path = Path(source.path)
        if not path.exists():
            raise PathNotFoundError(source.path)
        data = path.read_bytes()
        return data, _infer_mime(data, source.path)

    if isinstance(source, InlineSource):
        data = source.content.encode(source.encoding)
        return data, _infer_mime(data)

    # UrlSource — fetch over HTTP; prefer the response Content-Type header.
    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
        try:
            resp = await client.get(source.url)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise UrlFetchError(source.url, exc.response.status_code) from exc
        except httpx.RequestError as exc:
            raise UrlFetchError(source.url, None) from exc
    data = resp.content
    content_type = resp.headers.get("content-type", "").split(";")[0].strip()
    if not content_type or content_type == "application/octet-stream":
        content_type = _infer_mime(data, source.url)
    return data, content_type


async def intake_file(
    http: httpx.AsyncClient,
    request: IntakeRequest,
) -> uuid.UUID:
    """Verify namespace → resolve bytes → infer content type → upload to storage."""
    # 1. Verify the namespace exists.
    await _verify_namespace(http, request.namespace_id)

    # 2. Materialise bytes and infer content type from the source.
    data, content_type = await _resolve_bytes(request.source)

    # 3. Upload to the storage service.
    upload_resp = await http.post(
        f"/namespaces/{request.namespace_id}/objects",
        files={"file": (request.display_name, data, content_type)},
    )
    if upload_resp.status_code != status.HTTP_202_ACCEPTED:
        raise StorageServiceError(upload_resp.status_code, upload_resp.text)

    body = upload_resp.json()
    return uuid.UUID(body["task_id"])
