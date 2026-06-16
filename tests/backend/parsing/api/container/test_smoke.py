"""Container smoke tests for the parsing service.

These tests start the real ``artemis/backend-parsing:dev`` image alongside
Docling + MinIO testcontainers and verify the service works end-to-end under
the claim-check contract: POST /v1/parse parses the document, writes the
artifact to MinIO, and returns a ``BlobRef`` — the chunks are then read back
from MinIO to verify content.
"""

import json
import uuid
from pathlib import Path

import httpx
import pytest
from minio import Minio

from src.lib.core.ingestion.types import ChunkType, ParseArtifact

_HERE = Path(__file__).parent

# Minimal Markdown document — Docling supports .md natively.
_TEST_DOCUMENT = (
    b"# Test Document\n\nThis is a smoke test document with some content.\n"
)


def _metadata(obj_id: uuid.UUID | None = None) -> str:
    return json.dumps({"obj_id": str(obj_id or uuid.uuid4())})


def _read_artifact(minio_client: Minio, ref: dict) -> ParseArtifact:
    """Read the ParseArtifact (pages + chunks) the service wrote to MinIO."""
    response = minio_client.get_object(ref["bucket"], ref["key"])
    try:
        return ParseArtifact.model_validate_json(response.read())
    finally:
        response.close()
        response.release_conn()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_liveness_returns_200(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/liveness")
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readiness_returns_200(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/readiness")
    assert response.status_code == 200, response.json()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_markdown_writes_artifact(
    client: httpx.AsyncClient, minio_client: Minio
) -> None:
    """End-to-end parse: Docling processes the doc, the service writes the artifact
    to MinIO and returns its BlobRef; chunks are read back from MinIO."""
    obj_id = uuid.uuid4()
    response = await client.post(
        "/v1/parse",
        files={"file": ("test.md", _TEST_DOCUMENT, "text/markdown")},
        data={"metadata": _metadata(obj_id)},
    )
    assert response.status_code == 200, response.text
    ref = response.json()
    assert ref == {"bucket": "parsed-chunks", "key": f"parse/{obj_id}.json"}

    artifact = _read_artifact(minio_client, ref)
    chunks = artifact.chunks
    assert len(chunks) >= 1
    assert all(c.obj_id == obj_id for c in chunks)
    assert all(c.source == "test.md" for c in chunks)
    assert all(c.source != str(c.obj_id) for c in chunks)

    # Page parents are produced alongside the chunks and carry real Markdown.
    assert len(artifact.pages) >= 1
    assert any("Test Document" in p.markdown for p in artifact.pages)

    # The private replay cache holds the lossless DoclingDocument JSON.
    replay = minio_client.get_object("docling-replay", f"replay/{obj_id}.json")
    try:
        assert b"schema_name" in replay.read()
    finally:
        replay.close()
        replay.release_conn()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_pdf_writes_text_and_table_chunks(
    client: httpx.AsyncClient, minio_client: Minio
) -> None:
    """PDF with complex tables: validates both text and table ChunkTypes are produced."""
    pdf_bytes = (_HERE / "complex-tables.pdf").read_bytes()
    obj_id = uuid.uuid4()
    response = await client.post(
        "/v1/parse",
        files={"file": ("complex-tables.pdf", pdf_bytes, "application/pdf")},
        data={"metadata": _metadata(obj_id)},
    )
    assert response.status_code == 200, response.text
    chunks = _read_artifact(minio_client, response.json()).chunks
    assert len(chunks) >= 1

    chunk_types = {c.type for c in chunks}
    assert ChunkType.TEXT in chunk_types, "Expected at least one text chunk from PDF"
    assert (
        ChunkType.TABLE in chunk_types
    ), "Expected at least one table chunk from complex-tables.pdf"
    assert all(c.obj_id == obj_id for c in chunks)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_empty_file_writes_empty_artifact(
    client: httpx.AsyncClient, minio_client: Minio
) -> None:
    """An empty document should return 200 with a BlobRef to an empty artifact."""
    response = await client.post(
        "/v1/parse",
        files={"file": ("empty.md", b"", "text/markdown")},
        data={"metadata": _metadata()},
    )
    assert response.status_code == 200, response.text
    assert _read_artifact(minio_client, response.json()).chunks == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_unsupported_format_returns_400(client: httpx.AsyncClient) -> None:
    """
    Sending a format Docling cannot process → 400 (client error, not upstream fault).
    """
    response = await client.post(
        "/v1/parse",
        files={
            "file": ("binary.exe", b"\x4d\x5a\x90\x00" * 16, "application/octet-stream")
        },
        data={"metadata": _metadata()},
    )
    assert response.status_code == 400
    body = response.json()
    assert body["type"] == "document_processing_error"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_when_docling_down_returns_503(
    client_no_docling: httpx.AsyncClient,
) -> None:
    """When Docling is unreachable the service must return 503, not 500."""
    response = await client_no_docling.post(
        "/v1/parse",
        files={"file": ("test.md", _TEST_DOCUMENT, "text/markdown")},
        data={"metadata": _metadata()},
    )
    assert response.status_code == 503
    body = response.json()
    assert body["type"] == "upstream_service_error"
    assert body["service"] == "document-loader"
