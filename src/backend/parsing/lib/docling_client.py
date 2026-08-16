"""Thin async httpx facade over the docling-serve REST API.

Exposes submit-and-return-task_id operations only — no blocking waits.
The Celery layer owns the poll loop (via self.retry()); this client only
fires individual HTTP requests and raises on HTTP errors.

ServiceUnavailableError from persistent transport failures surfaces to the
parsing service endpoint as HTTP 503, which the worker's circuit breaker
handles.
"""

from __future__ import annotations

import json

import httpx
from docling.datamodel.document import DoclingDocument

from src.backend.parsing.lib.artifact import ParseStatus

__all__ = ["DoclingParseClient"]

# docling-serve ConversionStatus values that indicate a task has finished.
_TERMINAL: frozenset[str] = frozenset(
    {"success", "failure", "partial_success", "skipped"}
)


class DoclingParseClient:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    async def submit_source(
        self,
        *,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_secure: bool,
        bucket: str,
        key: str,
        target_bucket: str,
        target_key_prefix: str,
        callbacks: list[dict] | None = None,
        timeout: float = 120.0,
    ) -> str:
        """POST /v1/convert/source/batch with an S3 source AND S3 target → task_id.

        `/v1/convert/source/async`'s `sources` field is typed
        `FileSourceRequest | HttpSourceRequest` only — an S3 source there is a
        schema-level 422 (Pydantic discriminated-union rejection), not a policy
        toggle; confirmed directly against docling-serve v1.29.0's
        `docling.datamodel.service.requests.ConvertSourcesRequest`. Only the
        *batch* endpoint's `sources` union (`BatchConvertSourcesRequest`)
        includes `S3SourceRequest`. Despite the name, `/source/batch` is still
        a single-request, async, `task_id`-pollable call through the same
        orchestrator as `/source/async` — nothing else about the submit/poll/
        resolve flow changes.

        `BatchConvertSourcesRequest.target` has no inbody option (`S3Target |
        AzureBlobTarget | GoogleCloudStorageTarget | GoogleDriveTarget |
        PresignedUrlTarget`), so using this endpoint at all forces the result
        to come back via a target rather than inline. `PresignedUrlTarget`
        doesn't work against locally-deployed S3 clusters, so `S3Target` is
        used — the caller discovers the written key via a recursive listing
        (see `router._discover_s3_result_key`) since docling-jobkit nests the
        artifact under path segments not fully under our control.

        docling-serve both fetches the source and writes the converted JSON
        directly to S3 server-side — no document bytes cross this process in
        either direction. Requires docling-serve's Ray-serde patch (see
        `tools/oci/images/docling`) — without it, every S3-source submission
        fails instantly under the Ray engine (fixed upstream bug, not a
        schema/policy issue).
        """
        body = {
            # BatchConvertSourcesRequest.options defaults to to_formats=["md"]
            # (docling.datamodel.service.options.ConvertDocumentsOptions) --
            # without this, docling-serve writes a Markdown file to the S3Target
            # instead of the JSON this client/resolve_endpoint expects.
            "options": {"to_formats": ["json"]},
            "sources": [
                {
                    "kind": "s3",
                    "endpoint": s3_endpoint,
                    "access_key": s3_access_key,
                    "secret_key": s3_secret_key,
                    "verify_ssl": s3_secure,
                    "bucket": bucket,
                    "key_prefix": key,
                    # Defensive cap: this dispatch always targets exactly one
                    # known object, never a real prefix listing.
                    "max_num_elements": 1,
                }
            ],
            "target": {
                "kind": "s3",
                "endpoint": s3_endpoint,
                "access_key": s3_access_key,
                "secret_key": s3_secret_key,
                "verify_ssl": s3_secure,
                "bucket": target_bucket,
                "key_prefix": target_key_prefix,
            },
        }
        if callbacks:
            # BatchConvertSourcesRequest.callbacks: list[CallbackSpec] — fires
            # once with the terminal per-document result. Delivery is NOT
            # reliable (docling-serve retries 3x over ~1-2s then drops it
            # silently), so callers must keep polling as a safety net; this is
            # a fast path only, never a replacement for the poll loop.
            body["callbacks"] = callbacks
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.post("/v1/convert/source/batch", json=body)
            resp.raise_for_status()
            return resp.json()["task_id"]

    async def submit_file(
        self,
        content: bytes,
        filename: str,
        content_type: str,
        *,
        callbacks: list[dict] | None = None,
        timeout: float = 120.0,
    ) -> str:
        """POST /v1/convert/file/async → task_id (target=inbody, format=json).

        ``callbacks`` is a repeated ``callbacks`` form field on this endpoint
        (confirmed against docling-serve's ``process_file_async``, pinned
        commit ``69192d17``) — each value is either a bare URL or a
        JSON-encoded ``CallbackSpec``; httpx repeats the field once per list
        element when the ``data`` dict value is itself a list.
        """
        data: dict[str, object] = {"to_formats": "json"}
        if callbacks:
            data["callbacks"] = [json.dumps(cb) for cb in callbacks]
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.post(
                "/v1/convert/file/async",
                files={"files": (filename, content, content_type)},
                data=data,
            )
            resp.raise_for_status()
            return resp.json()["task_id"]

    async def get_status(self, task_id: str, *, timeout: float = 30.0) -> ParseStatus:
        """
        Non-blocking single status check (wait=0). Works for conversion and chunk tasks.
        """
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.get(f"/v1/status/poll/{task_id}", params={"wait": 0})
            resp.raise_for_status()
            data = resp.json()

        raw = data.get("task_status", "pending")
        server_error = data.get("error_message")
        if raw not in _TERMINAL:
            mapped = "processing"
            error_message = None
        elif raw == "success":
            mapped = "success"
            error_message = None
        elif raw == "partial_success":
            # Some page slices failed server-side (Ray engine fan-out). Treat as
            # failure so the Celery retry re-submits the whole document rather than
            # proceeding with a truncated DoclingDocument. Change to "success" here
            # if partial results become acceptable.
            mapped = "failure"
            error_message = (
                f"partial_success: {server_error}"
                if server_error
                else "partial_success"
            )
        elif raw == "skipped":
            # Unexpected in our flow (we always submit a fresh document). Fail loudly
            # rather than emit an empty artifact.
            mapped = "failure"
            error_message = f"skipped: {server_error}" if server_error else "skipped"
        else:
            mapped = "failure"
            error_message = server_error

        meta = data.get("task_meta") or {}
        return ParseStatus(
            status=mapped,  # type: ignore[arg-type]
            num_processed=meta.get("num_processed"),
            num_total=meta.get("num_total"),
            error_message=error_message,
        )

    async def fetch_conversion_result(
        self, task_id: str, *, timeout: float = 60.0
    ) -> DoclingDocument:
        """GET /v1/result/{task_id} → DoclingDocument (single-source mode only)."""
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.get(f"/v1/result/{task_id}")
            resp.raise_for_status()
            data = resp.json()
        return DoclingDocument.model_validate(data["document"]["json_content"])

    async def submit_chunk(
        self, doc_json_bytes: bytes, *, timeout: float = 120.0
    ) -> str:
        """POST /v1/chunk/hybrid/file/async (JSON_DOCLING input) → task_id.

        Uploads the DoclingDocument JSON inline (target=inbody). This is the
        only working option in docling-serve v1.29.0: no chunk endpoint
        accepts an S3 source (`BaseChunkDocumentsRequest.sources` is
        `FileSourceRequest | HttpSourceRequest` only — the same schema-level
        422 as convert's non-batch endpoint — and there's no batch variant
        for chunk to work around it with). The caller reads the
        replay-cached DoclingDocument JSON itself and passes the bytes here.
        """
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.post(
                "/v1/chunk/hybrid/file/async",
                files={"files": ("doc.json", doc_json_bytes, "application/json")},
            )
            resp.raise_for_status()
            return resp.json()["task_id"]

    async def fetch_chunk_result(
        self, task_id: str, *, timeout: float = 60.0
    ) -> list[dict]:
        """GET /v1/result/{task_id} → list of ChunkedDocumentResultItem dicts."""
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.get(f"/v1/result/{task_id}")
            resp.raise_for_status()
            return resp.json()["chunks"]
