"""Container smoke tests for the parsing service.

These tests start the real ``artemis/backend-parsing:dev`` image alongside
a Docling testcontainer and verify the service starts correctly and its HTTP
API works end-to-end: a real file is sent to POST /v1/parse and Docling
returns parsed chunks.
"""

from pathlib import Path

import httpx
import pytest
from pydantic import TypeAdapter

from src.lib.core.ingestion.types import ChunkType, ParsedChunk

_HERE = Path(__file__).parent

# Minimal Markdown document — Docling supports .md natively.
_TEST_DOCUMENT = (
    b"# Test Document\n\nThis is a smoke test document with some content.\n"
)

_chunks_adapter = TypeAdapter(list[ParsedChunk])


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
async def test_parse_markdown_returns_chunks(client: httpx.AsyncClient) -> None:
    """End-to-end parse: Docling processes the document, service returns ParsedChunks."""
    response = await client.post(
        "/v1/parse",
        files={"file": ("test.md", _TEST_DOCUMENT, "text/markdown")},
    )
    assert response.status_code == 200, response.json()
    chunks = _chunks_adapter.validate_python(response.json())
    assert len(chunks) >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_pdf_returns_text_and_table_chunks(
    client: httpx.AsyncClient,
) -> None:
    """PDF with complex tables: validates both text and table ChunkTypes are produced."""
    pdf_bytes = (_HERE / "complex-tables.pdf").read_bytes()
    response = await client.post(
        "/v1/parse",
        files={"file": ("complex-tables.pdf", pdf_bytes, "application/pdf")},
    )
    assert response.status_code == 200, response.json()
    chunks = _chunks_adapter.validate_python(response.json())
    assert len(chunks) >= 1

    chunk_types = {c.type for c in chunks}
    assert ChunkType.TEXT in chunk_types, "Expected at least one text chunk from PDF"
    assert (
        ChunkType.TABLE in chunk_types
    ), "Expected at least one table chunk from complex-tables.pdf"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_empty_file_returns_empty_list(client: httpx.AsyncClient) -> None:
    """An empty document should return 200 with an empty chunk list, not crash."""
    response = await client.post(
        "/v1/parse",
        files={"file": ("empty.md", b"", "text/markdown")},
    )
    assert response.status_code == 200, response.json()
    chunks = _chunks_adapter.validate_python(response.json())
    assert chunks == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_unsupported_format_returns_503(client: httpx.AsyncClient) -> None:
    """
    Sending a format Docling cannot process
    should return 503 (upstream rejected it).
    """
    response = await client.post(
        "/v1/parse",
        files={
            "file": ("binary.exe", b"\x4d\x5a\x90\x00" * 16, "application/octet-stream")
        },
    )
    assert response.status_code == 503
    body = response.json()
    assert body["type"] == "upstream_service_error"
    assert body["service"] == "document-loader"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parse_when_docling_down_returns_503(
    client_no_docling: httpx.AsyncClient,
) -> None:
    """When Docling is unreachable the service must return 503, not 500."""
    response = await client_no_docling.post(
        "/v1/parse",
        files={"file": ("test.md", _TEST_DOCUMENT, "text/markdown")},
    )
    assert response.status_code == 503
    body = response.json()
    assert body["type"] == "upstream_service_error"
    assert body["service"] == "document-loader"
