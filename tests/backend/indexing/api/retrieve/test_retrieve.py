# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Integration tests for the POST /retrieve/invoke LangServe endpoint.

Uses FastAPI's TestClient with all infrastructure mocked.  The retrieve
service singleton is injected directly via monkeypatch so no real
vectorstore or embeddings service is needed.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from langchain_core.documents import Document

from src.backend.indexing.api.main import app
import src.backend.indexing.api.retrieve.service as retrieve_service


_NS = str(uuid.uuid4())
_SAMPLE_DOCS = [
    Document(page_content="relevant result", metadata={"namespace_id": _NS}),
]


@pytest.fixture
def mock_retriever():
    m = MagicMock()
    m.invoke.return_value = _SAMPLE_DOCS
    return m


@pytest.fixture
def client(mock_retriever, monkeypatch):
    """TestClient with the retrieve singleton replaced by a mock retriever."""
    monkeypatch.setattr(retrieve_service, "_retriever", mock_retriever)
    with TestClient(app) as c:
        yield c


def _invoke_body(namespace_id: str | None = _NS, extra: dict | None = None) -> dict:
    configurable: dict = {}
    if namespace_id is not None:
        configurable["namespace_id"] = namespace_id
    if extra:
        configurable.update(extra)
    return {"input": "test query", "config": {"configurable": configurable}}


class TestRetrieveEndpoint:
    def test_invoke_returns_documents(self, client: TestClient):
        response = client.post("/retrieve/invoke", json=_invoke_body())
        assert response.status_code == 200
        body = response.json()
        assert "output" in body
        assert len(body["output"]) == 1
        assert body["output"][0]["page_content"] == "relevant result"

    def test_invoke_missing_namespace_id_returns_400(self, client: TestClient):
        response = client.post("/retrieve/invoke", json=_invoke_body(namespace_id=None))
        assert response.status_code == 400

    def test_invoke_passes_group_id_to_retriever(
        self, client: TestClient, mock_retriever: MagicMock
    ):
        group_id = str(uuid.uuid4())
        client.post("/retrieve/invoke", json=_invoke_body(extra={"group_id": group_id}))
        config_passed = (
            mock_retriever.invoke.call_args.kwargs.get("config")
            or mock_retriever.invoke.call_args.args[1]
        )
        assert config_passed["configurable"]["group_id"] == group_id

    def test_service_not_initialized_returns_500(self, monkeypatch):
        monkeypatch.setattr(retrieve_service, "_retriever", None)
        # raise_server_exceptions=False makes TestClient return the 500 response
        # rather than re-raising the RuntimeError in the test process.
        with TestClient(app, raise_server_exceptions=False) as c:
            response = c.post("/retrieve/invoke", json=_invoke_body())
        assert response.status_code == 500
