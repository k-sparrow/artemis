"""Business logic for the enterprise intake endpoint.

Flow:
  1. Verify namespace_id exists on the storage service (GET /namespaces/{id}).
  2. Materialise bytes from the source:
       filesystem — read from mounted FS path
       inline     — encode text content to bytes
       url        — fetch over HTTP
  3. For filesystem sources, claim (namespace_id, canonical path, sha256) in
     the dedup ledger — see dedup.py. A losing claim short-circuits with the
     existing task_id instead of re-uploading.
  4. POST bytes to the storage service upload endpoint.
  5. Backfill the dedup ledger's task_id (filesystem sources only).

The namespace UUID is always provided by the caller — data_sources owns
namespace creation and injects the UUID into the Kafka message headers.
This service has zero knowledge of MinIO, Kafka, Qdrant, or Celery internals.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import httpx
import mimetypes

import filetype
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.enterprise.intake.api.config import settings
from src.backend.enterprise.intake.api.intake import dedup
from src.backend.enterprise.intake.api.intake.exceptions import (
    NamespaceNotFoundError,
    PathEscapesWatchRootError,
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


async def _verify_namespace(
    http: httpx.AsyncClient,
    namespace_id: uuid.UUID,
    owner_id: uuid.UUID,
) -> None:
    """Verify the namespace exists and is accessible by the caller."""
    resp = await http.get(
        f"/namespaces/{namespace_id}",
        headers={"X-Owner-Id": str(owner_id)},
    )
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


def _canonical_watch_path(path: str) -> str:
    """Resolve *path* to its real filesystem identity (following symlinks).

    A symlink and its target must collapse to the same dedup key (see
    dedup.py) — otherwise the connector's own path-based idempotency gap
    (a symlink and its target are "different files" to Camel) reopens here.
    Also enforces that the resolved path stays under settings.WATCH_ROOT: a
    symlink pointing outside the mounted watch tree is rejected rather than
    silently followed.
    """
    resolved = Path(path).resolve()
    root = Path(settings.WATCH_ROOT).resolve()
    if not resolved.is_relative_to(root):
        raise PathEscapesWatchRootError(path, str(resolved))
    return str(resolved)


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
    session: AsyncSession | None = None,
) -> uuid.UUID:
    """Verify namespace → resolve bytes → dedup claim → upload → backfill.

    ``session`` is optional so InlineSource/UrlSource callers (and any test
    not exercising the filesystem path) don't need a DB session at all — the
    dedup ledger only applies to FilesystemSource, where connector-level
    duplication actually originates.
    """
    # 1. Verify the namespace exists.
    await _verify_namespace(http, request.namespace_id, request.owner_id)

    # 2. Materialise bytes and infer content type from the source.
    data, content_type = await _resolve_bytes(request.source)

    dedup_key: tuple[str, str] | None = None
    if isinstance(request.source, FilesystemSource) and session is not None:
        canonical_path = _canonical_watch_path(request.source.path)
        sha256 = hashlib.sha256(data).hexdigest()
        dedup_key = (canonical_path, sha256)

        claim = await dedup.claim_or_get_existing(
            session,
            namespace_id=request.namespace_id,
            path=canonical_path,
            sha256=sha256,
        )
        if not claim.won:
            existing_task_id = claim.existing_task_id or await dedup.wait_for_task_id(
                session,
                namespace_id=request.namespace_id,
                path=canonical_path,
                sha256=sha256,
            )
            if existing_task_id is not None:
                return existing_task_id
            # The claimant never backfilled within the wait window (likely
            # failed before completing its upload) — fail open and proceed
            # with our own upload rather than drop this request on the floor.
            dedup_key = None

    # 3. Upload to the storage service.
    params = {}
    if request.group_id is not None:
        params["group_id"] = str(request.group_id)
    upload_resp = await http.post(
        f"/namespaces/{request.namespace_id}/objects",
        files={"file": (request.display_name, data, content_type)},
        params=params,
        headers={"X-Owner-Id": str(request.owner_id)},
    )
    if upload_resp.status_code != status.HTTP_202_ACCEPTED:
        raise StorageServiceError(upload_resp.status_code, upload_resp.text)

    body = upload_resp.json()
    task_id = uuid.UUID(body["task_id"])

    if dedup_key is not None and session is not None:
        canonical_path, sha256 = dedup_key
        await dedup.backfill_task_id(
            session,
            namespace_id=request.namespace_id,
            path=canonical_path,
            sha256=sha256,
            task_id=task_id,
        )

    return task_id
