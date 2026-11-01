"""Unit tests for DoclingParseClient's S3 chunk submission.

The request is validated against docling's own request model rather than
compared to a literal body, so these fail if docling-serve would reject it.
"""

from __future__ import annotations

import json

import pytest
import respx
from docling.datamodel.service.chunking import HybridChunkerOptions
from docling.datamodel.service.requests import BatchConvertSourcesRequest
from httpx import Response

from src.backend.parsing.lib.docling_client import DoclingParseClient

_BASE_URL = "http://docling-serve:5001"
_COORDINATES = {
    "s3_endpoint": "minio:9000",
    "s3_access_key": "access",
    "s3_secret_key": "secret",
    "s3_secure": False,
    "bucket": "docling-replay",
    "key": "replay/doc.json",
    "target_bucket": "docling-replay",
    "target_key_prefix": "scratch/chunk/doc/",
}


@pytest.fixture
def batch_route():
    with respx.mock(base_url=_BASE_URL) as router:
        yield router.post("/v1/convert/source/batch").mock(
            return_value=Response(200, json={"task_id": "chunk-task"})
        )


class TestSubmitChunk:
    async def test_returns_the_task_id(self, batch_route: respx.Route) -> None:
        task_id = await DoclingParseClient(_BASE_URL).submit_chunk(**_COORDINATES)

        assert task_id == "chunk-task"

    async def test_requests_only_chunks_from_the_replay_json(
        self, batch_route: respx.Route
    ) -> None:
        await DoclingParseClient(_BASE_URL).submit_chunk(**_COORDINATES)

        body = json.loads(batch_route.calls.last.request.content)
        request = BatchConvertSourcesRequest.model_validate(body)
        assert [f.value for f in request.options.to_formats] == ["chunks"]
        assert [f.value for f in request.options.from_formats] == ["json_docling"]
        assert body["sources"][0]["bucket"] == "docling-replay"
        assert body["sources"][0]["key_prefix"] == "replay/doc.json"
        assert body["sources"][0]["max_num_elements"] == 1
        assert body["target"]["key_prefix"] == "scratch/chunk/doc/"

    async def test_uses_the_chunker_the_chunk_endpoint_defaulted_to(
        self, batch_route: respx.Route
    ) -> None:
        """Changing this changes every document's chunks, so it's pinned to
        the defaults /v1/chunk/hybrid applied."""
        await DoclingParseClient(_BASE_URL).submit_chunk(**_COORDINATES)

        body = json.loads(batch_route.calls.last.request.content)
        request = BatchConvertSourcesRequest.model_validate(body)
        assert request.options.chunking_options == HybridChunkerOptions()
