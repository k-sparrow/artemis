"""Unit tests for the enterprise intake endpoint.

All external I/O is mocked — no real filesystem paths, HTTP servers, or storage service.
Tests cover:
  - Happy path for each source type (filesystem, inline, url)
  - Error paths: missing file (404), URL fetch failure (502), storage service error (502),
    namespace not found (422)
  - Request validation (missing fields → 422)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest  # noqa: F401
from fastapi import status
from fastapi.testclient import TestClient

import uuid

from tests.backend.enterprise.intake.api.intake.conftest import (
    _NAMESPACE_ID,
    _OWNER_ID,
    _TASK_ID,
)

_GROUP_ID = uuid.uuid4()

_COMMON = {
    "display_name": "doc.pdf",
    "namespace_id": str(_NAMESPACE_ID),
    "owner_id": str(_OWNER_ID),
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
        assert body["namespace_id"] == str(_NAMESPACE_ID)

    def test_namespace_verified_on_storage(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")

        _post(client, {"type": "filesystem", "path": str(f)})

        mock_http.get.assert_called_once()
        assert mock_http.get.call_args.args[0] == f"/namespaces/{_NAMESPACE_ID}"

    def test_file_bytes_uploaded_to_storage(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"pdf content")

        _post(client, {"type": "filesystem", "path": str(f)})

        upload_call = mock_http.post.call_args_list[0]
        assert f"/namespaces/{_NAMESPACE_ID}/objects" in upload_call.args[0]
        files = upload_call.kwargs["files"]
        assert files["file"][0] == "doc.pdf"
        assert files["file"][1] == b"pdf content"
        # filetype cannot identify arbitrary bytes -
        # falls back to mimetypes (filename ext)
        assert files["file"][2] == "application/pdf"

    def test_owner_id_forwarded_as_header(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")

        _post(client, {"type": "filesystem", "path": str(f)})

        upload_call = mock_http.post.call_args_list[0]
        headers = upload_call.kwargs.get("headers", {})
        assert headers.get("X-Owner-Id") == str(_OWNER_ID)

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

        files = mock_http.post.call_args_list[0].kwargs["files"]
        assert files["file"][1] == b"Hello world"
        # filetype cannot identify plain text — falls back to application/octet-stream
        assert files["file"][2] == "application/octet-stream"

    def test_custom_encoding_used(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        _post(client, {"type": "inline", "content": "café", "encoding": "latin-1"})

        files = mock_http.post.call_args_list[0].kwargs["files"]
        assert files["file"][1] == "café".encode("latin-1")

    def test_default_encoding_is_utf8(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        _post(client, {"type": "inline", "content": "café"})

        files = mock_http.post.call_args_list[0].kwargs["files"]
        assert files["file"][1] == "café".encode("utf-8")


# ---------------------------------------------------------------------------
# URL source
# ---------------------------------------------------------------------------


class TestUrlSource:
    def _patch_url_client(
        self, content: bytes, content_type: str = "", side_effect=None
    ) -> MagicMock:
        url_resp = MagicMock()
        url_resp.content = content
        url_resp.headers = {"content-type": content_type} if content_type else {}
        url_resp.raise_for_status = MagicMock()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_ctx)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_ctx.get = AsyncMock(return_value=url_resp if not side_effect else None)
        if side_effect:
            mock_ctx.get = AsyncMock(side_effect=side_effect)

        mock_cls = MagicMock(return_value=mock_ctx)
        return mock_cls

    def test_happy_path_returns_202(self, client: TestClient) -> None:
        mock_cls = self._patch_url_client(b"<html>page</html>")

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient",
            mock_cls,
        ):
            resp = _post(client, {"type": "url", "url": "https://example.com/doc"})

        assert resp.status_code == status.HTTP_202_ACCEPTED
        body = resp.json()
        assert body["task_id"] == str(_TASK_ID)

    def test_fetched_bytes_uploaded_to_storage(
        self, client: TestClient, mock_http: MagicMock
    ) -> None:
        mock_cls = self._patch_url_client(
            b"<html>page content</html>", content_type="text/html"
        )

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient",
            mock_cls,
        ):
            _post(client, {"type": "url", "url": "https://example.com/doc"})

        files = mock_http.post.call_args_list[0].kwargs["files"]
        assert files["file"][1] == b"<html>page content</html>"
        assert files["file"][2] == "text/html"

    def test_http_error_from_url_returns_502(self, client: TestClient) -> None:
        import httpx as _httpx

        error_resp = MagicMock()
        error_resp.status_code = status.HTTP_403_FORBIDDEN
        exc = _httpx.HTTPStatusError("403", request=MagicMock(), response=error_resp)
        mock_cls = self._patch_url_client(b"", side_effect=exc)

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient",
            mock_cls,
        ):
            resp = _post(client, {"type": "url", "url": "https://example.com/secret"})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY

    def test_connection_error_from_url_returns_502(self, client: TestClient) -> None:
        import httpx as _httpx

        exc = _httpx.RequestError("connection refused", request=MagicMock())
        mock_cls = self._patch_url_client(b"", side_effect=exc)

        with patch(
            "src.backend.enterprise.intake.api.intake.service.httpx.AsyncClient",
            mock_cls,
        ):
            resp = _post(client, {"type": "url", "url": "https://unreachable.example"})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY


# ---------------------------------------------------------------------------
# group_id forwarding
# ---------------------------------------------------------------------------


class TestGroupIdForwarding:
    def test_group_id_forwarded_as_query_param(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")

        client.post(
            "/intake",
            json={
                **_COMMON,
                "group_id": str(_GROUP_ID),
                "source": {"type": "filesystem", "path": str(f)},
            },
        )

        upload_call = mock_http.post.call_args_list[0]
        params = upload_call.kwargs.get("params", {})
        assert params.get("group_id") == str(_GROUP_ID)

    def test_no_group_id_sends_no_query_param(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")

        _post(client, {"type": "filesystem", "path": str(f)})

        upload_call = mock_http.post.call_args_list[0]
        params = upload_call.kwargs.get("params", {})
        assert not params


# ---------------------------------------------------------------------------
# Storage service errors
# ---------------------------------------------------------------------------


class TestStorageServiceErrors:
    def test_namespace_not_found_returns_422(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        not_found = MagicMock()
        not_found.status_code = status.HTTP_404_NOT_FOUND
        mock_http.get = AsyncMock(return_value=not_found)

        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")
        resp = _post(client, {"type": "filesystem", "path": str(f)})

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert str(_NAMESPACE_ID) in resp.json()["detail"]

    def test_namespace_check_storage_error_returns_502(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        err_resp = MagicMock()
        err_resp.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        err_resp.text = "internal error"
        mock_http.get = AsyncMock(return_value=err_resp)

        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")
        resp = _post(client, {"type": "filesystem", "path": str(f)})

        assert resp.status_code == status.HTTP_502_BAD_GATEWAY
        assert "Storage service error" in resp.json()["detail"]

    def test_upload_failure_returns_502(
        self, client: TestClient, mock_http: MagicMock, tmp_path: Path
    ) -> None:
        upload_err = MagicMock()
        upload_err.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        upload_err.text = "service unavailable"
        mock_http.post = AsyncMock(return_value=upload_err)

        f = tmp_path / "doc.pdf"
        f.write_bytes(b"content")
        resp = _post(client, {"type": "filesystem", "path": str(f)})

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
                "namespace_id": str(_NAMESPACE_ID),
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_missing_namespace_id_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/intake",
            json={
                "source": {"type": "inline", "content": "text"},
                "display_name": "doc.txt",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_invalid_namespace_id_uuid_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/intake",
            json={
                **_COMMON,
                "namespace_id": "not-a-uuid",
                "source": {"type": "inline", "content": "x"},
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
