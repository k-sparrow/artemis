"""Unit tests for the enterprise intake endpoint.

All external I/O is mocked — no real filesystem paths, HTTP servers, or storage service.
Tests cover:
  - Happy path for each source type (filesystem, inline, url)
  - Error paths: missing file (404), URL fetch failure (502), storage service error (502)
  - Request validation (missing fields → 422)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest  # noqa: F401
from fastapi import status
from fastapi.testclient import TestClient

from tests.backend.enterprise.intake.conftest import (
    NAMESPACE_RESPONSE,
    _NAMESPACE_ID,
    _S3_KEY,
    _TASK_ID,
)

_COMMON = {
    "display_name": "doc.pdf",
    "content_type": "application/pdf",
    "namespace": "acme",
    "org_name": "acme-corp",
}


def _post(client: TestClient, source: dict) -> dict:
    return client.post("/intake", json={**_COMMON, "source": source})


# ---------------------------------------------------------------------------
# Filesystem source
# ---------------------------------------------------------------------------


class TestFilesystemSource:
    def test_happy_path_returns_202(self, client: TestClient, tmp_path: Path) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"pdf content")

        resp = _post(client, {"type": "filesystem", "path": str(f)})

        assert resp.status_code == status.HTTP_202_ACCEPTED
        body = resp.json()
        assert body["task_id"] == str(_TASK_ID)
        assert body["s3_key"] == _S3_KEY
        assert body["namespace_id"] == str(_NAMESPACE_ID)

    def test_namespace_upserted_with_correct_payload(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")

        _post(client, {"type": "filesystem", "path": str(f)})

        ns_call = mock_http.post.call_args_list[0]
        assert ns_call.args[0] == "/namespaces"
        assert ns_call.kwargs["json"] == {"type": "shared", "name": "acme"}

    def test_file_bytes_uploaded_to_storage(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"pdf content")

        _post(client, {"type": "filesystem", "path": str(f)})

        upload_call = mock_http.post.call_args_list[1]
        assert f"/namespaces/{_NAMESPACE_ID}/objects" in upload_call.args[0]
        files = upload_call.kwargs["files"]
        assert files["file"][0] == "doc.pdf"
        assert files["file"][1] == b"pdf content"

    def test_missing_file_returns_404(self, client: TestClient) -> None:
        resp = _post(
            client, {"type": "filesystem", "path": "/nonexistent/path/doc.pdf"}
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Inline source
# ---------------------------------------------------------------------------


class TestInlineSource:
    def test_happy_path_returns_202(self, client: TestClient) -> None:
        resp = _post(client, {"type": "inline", "content": "Hello world"})

        assert resp.status_code == status.HTTP_202_ACCEPTED
        body = resp.json()
        assert body["task_id"] == str(_TASK_ID)
        assert body["namespace_id"] == str(_NAMESPACE_ID)

    def test_content_encoded_to_bytes(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        _post(client, {"type": "inline", "content": "Hello world"})

        upload_call = mock_http.post.call_args_list[1]
        files = upload_call.kwargs["files"]
        assert files["file"][1] == b"Hello world"

    def test_custom_encoding_used(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        _post(client, {"type": "inline", "content": "café", "encoding": "latin-1"})

        upload_call = mock_http.post.call_args_list[1]
        files = upload_call.kwargs["files"]
        assert files["file"][1] == "café".encode("latin-1")

    def test_default_encoding_is_utf8(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        _post(client, {"type": "inline", "content": "café"})

        upload_call = mock_http.post.call_args_list[1]
        files = upload_call.kwargs["files"]
        assert files["file"][1] == "café".encode("utf-8")


# ---------------------------------------------------------------------------
# URL source
# ---------------------------------------------------------------------------


class TestUrlSource:
    def test_happy_path_returns_202(self, client: TestClient) -> None:
        url_resp = MagicMock()
        url_resp.content = b"<html>page</html>"
        url_resp.raise_for_status = MagicMock()

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient"
        ) as mock_cls:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_ctx.get = AsyncMock(return_value=url_resp)
            mock_cls.return_value = mock_ctx

            resp = _post(client, {"type": "url", "url": "https://example.com/doc"})

        assert resp.status_code == status.HTTP_202_ACCEPTED

    def test_fetched_bytes_uploaded_to_storage(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        url_resp = MagicMock()
        url_resp.content = b"<html>page content</html>"
        url_resp.raise_for_status = MagicMock()

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient"
        ) as mock_cls:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_ctx.get = AsyncMock(return_value=url_resp)
            mock_cls.return_value = mock_ctx

            _post(client, {"type": "url", "url": "https://example.com/doc"})

        upload_call = mock_http.post.call_args_list[1]
        assert upload_call.kwargs["files"]["file"][1] == b"<html>page content</html>"

    def test_http_error_from_url_returns_502(self, client: TestClient) -> None:
        import httpx as _httpx

        error_resp = MagicMock()
        error_resp.status_code = status.HTTP_403_FORBIDDEN

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient"
        ) as mock_cls:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_ctx.get = AsyncMock(
                side_effect=_httpx.HTTPStatusError(
                    "403", request=MagicMock(), response=error_resp
                )
            )
            mock_cls.return_value = mock_ctx

            resp = _post(client, {"type": "url", "url": "https://example.com/secret"})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY

    def test_connection_error_from_url_returns_502(self, client: TestClient) -> None:
        import httpx as _httpx

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient"
        ) as mock_cls:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
            mock_ctx.__aexit__ = AsyncMock(return_value=False)
            mock_ctx.get = AsyncMock(
                side_effect=_httpx.RequestError(
                    "connection refused", request=MagicMock()
                )
            )
            mock_cls.return_value = mock_ctx

            resp = _post(client, {"type": "url", "url": "https://unreachable.example"})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY


# ---------------------------------------------------------------------------
# Storage service errors
# ---------------------------------------------------------------------------


class TestStorageServiceErrors:
    def test_namespace_upsert_failure_returns_502(
        self, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        from src.backend.enterprise.intake.api.main import app

        err_resp = MagicMock()
        err_resp.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        err_resp.text = "internal error"
        mock_http.post = AsyncMock(return_value=err_resp)

        with TestClient(app) as c:
            app.state.http_client = mock_http
            f = tmp_path / "doc.pdf"
            f.write_bytes(b"content")
            resp = _post(c, {"type": "filesystem", "path": str(f)})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY
        assert "Storage service error" in resp.json()["detail"]

    def test_upload_failure_returns_502(
        self, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        from src.backend.enterprise.intake.api.main import app

        ns_resp = MagicMock()
        ns_resp.status_code = status.HTTP_201_CREATED
        ns_resp.json.return_value = NAMESPACE_RESPONSE
        upload_err = MagicMock()
        upload_err.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        upload_err.text = "service unavailable"
        mock_http.post = AsyncMock(side_effect=[ns_resp, upload_err])

        with TestClient(app) as c:
            app.state.http_client = mock_http
            f = tmp_path / "doc.pdf"
            f.write_bytes(b"content")
            resp = _post(c, {"type": "filesystem", "path": str(f)})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY


# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------


class TestRequestValidation:
    def test_missing_source_returns_422(self, client: TestClient) -> None:
        resp = client.post("/intake", json={**_COMMON})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_unknown_source_type_returns_422(self, client: TestClient) -> None:
        resp = _post(client, {"type": "unknown"})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_missing_display_name_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/intake",
            json={
                "source": {"type": "inline", "content": "text"},
                "content_type": "text/plain",
                "namespace": "acme",
                "org_name": "acme-corp",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_filesystem_missing_path_returns_422(self, client: TestClient) -> None:
        resp = _post(client, {"type": "filesystem"})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_url_missing_url_returns_422(self, client: TestClient) -> None:
        resp = _post(client, {"type": "url"})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_inline_missing_content_returns_422(self, client: TestClient) -> None:
        resp = _post(client, {"type": "inline"})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
