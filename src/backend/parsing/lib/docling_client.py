"""Thin async httpx facade over the docling-serve REST API.

Exposes submit-and-return-task_id operations only — no blocking waits.
The Celery layer owns the poll loop (via self.retry()); this client only
fires individual HTTP requests and raises on HTTP errors.

ServiceUnavailableError from persistent transport failures surfaces to the
parsing service endpoint as HTTP 503, which the worker's circuit breaker
handles.
"""

from __future__ import annotations

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

    async def submit_file(
        self,
        content: bytes,
        filename: str,
        content_type: str,
        *,
        timeout: float = 120.0,
    ) -> str:
        """POST /v1/convert/file/async → task_id (target=inbody, format=json)."""
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.post(
                "/v1/convert/file/async",
                files={"files": (filename, content, content_type)},
                data={"to_formats": "json"},
            )
            resp.raise_for_status()
            return resp.json()["task_id"]

    async def submit_batch(
        self,
        *,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_secure: bool,
        src_bucket: str,
        src_prefix: str,
        dst_bucket: str,
        dst_prefix: str,
        timeout: float = 120.0,
    ) -> str:
        """POST /v1/convert/source/batch with S3SourceRequest + S3Target → task_id.

        Submits all shard PDFs at src_prefix as a batch S3 conversion.
        Results land at dst_bucket under dst_prefix, but docling_jobkit appends
        a 12-char SHA-256 hash of each source URI before the shard filename:
            {dst_prefix}{sha256(s3://{src_bucket}/{shard_key})[:12]}/{shard}.json
        There is no API knob to disable this behaviour (hardcoded in
        docling_jobkit._upload_document_to_s3_target). resolve_endpoint absorbs
        the extra directory level via a recursive listing of dst_prefix.
        TODO: drop the hash workaround once IBM fixes docling-jobkit to honour
              dst_prefix verbatim (track upstream issue in
              docling-project/docling-jobkit).
        """
        s3_creds = {
            "endpoint": s3_endpoint,
            "access_key": s3_access_key,
            "secret_key": s3_secret_key,
            "verify_ssl": s3_secure,
        }
        body = {
            "options": {"to_formats": ["json"]},
            "sources": [
                {
                    "kind": "s3",
                    **s3_creds,
                    "bucket": src_bucket,
                    "key_prefix": src_prefix,
                }
            ],
            "target": {
                "kind": "s3",
                **s3_creds,
                "bucket": dst_bucket,
                "key_prefix": dst_prefix,
            },
        }
        async with httpx.AsyncClient(
            base_url=self._base_url, timeout=timeout
        ) as client:
            resp = await client.post("/v1/convert/source/batch", json=body)
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
            # Some shards failed → incomplete S3 results. Treat as failure so the
            # Celery retry re-submits the whole batch rather than proceeding with a
            # truncated DoclingDocument. Change to "success" here if partial results
            # become acceptable.
            mapped = "failure"
            error_message = (
                f"partial_success: {server_error}"
                if server_error
                else "partial_success"
            )
        elif raw == "skipped":
            # Unexpected in our flow (we always submit fresh shard PDFs). Fail loudly
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
        """POST /v1/chunk/hybrid/file/async (JSON_DOCLING input) → task_id."""
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
