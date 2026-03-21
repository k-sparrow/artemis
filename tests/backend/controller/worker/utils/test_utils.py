"""Unit tests for controller worker utility functions.

All external dependencies (MinIO, HTTP services) are replaced with mocks —
no infrastructure is required.
"""

from __future__ import annotations

import logging
import uuid
from unittest.mock import MagicMock

import pytest
import respx
from httpx import Response

from src.backend.controller.lib.schemas import S3Details, SourceDetails
import pybreaker

from src.backend.controller.worker.utils import (
    call_indexing_service,
    call_parsing_service,
    fetch_from_s3,
    parsing_breaker,
)
from src.lib.core.ingestion.types import ChunkType, ParsedChunk

_logger = logging.getLogger(__name__)

_PARSING_URL = "http://test-parsing:10001"
_INDEXING_URL = "http://test-indexing:10000"

_SAMPLE_CHUNKS = [
    ParsedChunk(page_content="chunk one", source="test.md", type=ChunkType.TEXT),
    ParsedChunk(page_content="| col |", source="test.md", type=ChunkType.TABLE),
]

_SAMPLE_CHUNKS_JSON = [
    {"page_content": "chunk one", "source": "test.md", "type": "text"},
    {"page_content": "| col |", "source": "test.md", "type": "table"},
]


# ---------------------------------------------------------------------------
# fetch_from_s3
# ---------------------------------------------------------------------------


class TestFetchFromS3:
    def _s3(self) -> S3Details:
        return S3Details(bucket="my-bucket", object="docs/test.pdf")

    def test_returns_raw_bytes(self) -> None:
        mock_response = MagicMock()
        mock_response.read.return_value = b"file content"
        mock_client = MagicMock()
        mock_client.get_object.return_value = mock_response

        result = fetch_from_s3(mock_client, self._s3(), _logger)

        assert result == b"file content"
        mock_client.get_object.assert_called_once_with("my-bucket", "docs/test.pdf")

    def test_minio_error_propagates(self) -> None:
        mock_client = MagicMock()
        mock_client.get_object.side_effect = Exception("connection refused")

        with pytest.raises(Exception, match="connection refused"):
            fetch_from_s3(mock_client, self._s3(), _logger)


# ---------------------------------------------------------------------------
# call_parsing_service
# ---------------------------------------------------------------------------


class TestCallParsingService:
    def _source(self, filename: str = "test.md") -> SourceDetails:
        return SourceDetails(path=filename, content_type="text/markdown")

    @respx.mock
    def test_happy_path_returns_parsed_chunks(self) -> None:
        respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(200, json=_SAMPLE_CHUNKS_JSON)
        )

        chunks = call_parsing_service(
            file_bytes=b"# Hello",
            source=self._source(),
            parsing_url=_PARSING_URL,
            timeout=5.0,
            logger=_logger,
        )

        assert len(chunks) == 2
        assert chunks[0].page_content == "chunk one"
        assert chunks[0].type == ChunkType.TEXT
        assert chunks[1].type == ChunkType.TABLE

    @respx.mock
    def test_filename_forwarded_as_multipart_name(self) -> None:
        route = respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(200, json=[_SAMPLE_CHUNKS_JSON[0]])
        )

        call_parsing_service(
            file_bytes=b"content",
            source=self._source("report.pdf"),
            parsing_url=_PARSING_URL,
            timeout=5.0,
            logger=_logger,
        )

        request = route.calls.last.request
        # multipart body must contain the filename
        assert b"report.pdf" in request.content

    @respx.mock
    def test_content_type_forwarded(self) -> None:
        route = respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(200, json=[_SAMPLE_CHUNKS_JSON[0]])
        )

        call_parsing_service(
            file_bytes=b"content",
            source=SourceDetails(path="doc.pdf", content_type="application/pdf"),
            parsing_url=_PARSING_URL,
            timeout=5.0,
            logger=_logger,
        )

        request = route.calls.last.request
        assert b"application/pdf" in request.content

    @respx.mock
    def test_non_200_raises_http_status_error(self) -> None:
        respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(503, json={"detail": "docling down"})
        )

        with pytest.raises(Exception):
            call_parsing_service(
                file_bytes=b"content",
                source=self._source(),
                parsing_url=_PARSING_URL,
                timeout=5.0,
                logger=_logger,
            )

    @respx.mock
    def test_source_path_none_falls_back_to_document(self) -> None:
        route = respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(200, json=[_SAMPLE_CHUNKS_JSON[0]])
        )

        call_parsing_service(
            file_bytes=b"content",
            source=SourceDetails(path=None, content_type="application/octet-stream"),
            parsing_url=_PARSING_URL,
            timeout=5.0,
            logger=_logger,
        )

        request = route.calls.last.request
        assert b"document" in request.content


# ---------------------------------------------------------------------------
# call_indexing_service
# ---------------------------------------------------------------------------


class TestCallIndexingService:
    _NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")
    _RESULT = {"num_added": 2, "num_updated": 0, "num_skipped": 0, "ids": ["a", "b"]}

    @respx.mock
    def test_happy_path_returns_result_dict(self) -> None:
        respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(200, json=self._RESULT)
        )

        result = call_indexing_service(
            chunks=_SAMPLE_CHUNKS,
            namespace_id=self._NAMESPACE,
            ingestion_url=_INDEXING_URL,
            timeout=5.0,
            logger=_logger,
        )

        assert result == self._RESULT

    @respx.mock
    def test_namespace_sent_as_query_param(self) -> None:
        route = respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(200, json=self._RESULT)
        )

        call_indexing_service(
            chunks=_SAMPLE_CHUNKS,
            namespace_id=self._NAMESPACE,
            ingestion_url=_INDEXING_URL,
            timeout=5.0,
            logger=_logger,
        )

        request = route.calls.last.request
        assert str(self._NAMESPACE) in str(request.url)

    @respx.mock
    def test_chunks_serialised_as_json_body(self) -> None:
        route = respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(200, json=self._RESULT)
        )

        call_indexing_service(
            chunks=_SAMPLE_CHUNKS,
            namespace_id=self._NAMESPACE,
            ingestion_url=_INDEXING_URL,
            timeout=5.0,
            logger=_logger,
        )

        import json

        body = json.loads(route.calls.last.request.content)
        assert len(body) == 2
        assert body[0]["page_content"] == "chunk one"
        assert body[0]["type"] == "text"
        assert body[1]["type"] == "table"

    @respx.mock
    def test_non_200_raises(self) -> None:
        respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(422, json={"detail": "bad chunks"})
        )

        with pytest.raises(Exception):
            call_indexing_service(
                chunks=_SAMPLE_CHUNKS,
                namespace_id=self._NAMESPACE,
                ingestion_url=_INDEXING_URL,
                timeout=5.0,
                logger=_logger,
            )


# ---------------------------------------------------------------------------
# Circuit breakers
# ---------------------------------------------------------------------------


def _parse(url: str = _PARSING_URL) -> None:
    call_parsing_service(
        file_bytes=b"x",
        source=SourceDetails(path="f.md", content_type="text/markdown"),
        parsing_url=url,
        timeout=5.0,
        logger=_logger,
    )


def _index(url: str = _INDEXING_URL) -> None:
    call_indexing_service(
        chunks=_SAMPLE_CHUNKS,
        namespace_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        ingestion_url=url,
        timeout=5.0,
        logger=_logger,
    )


class TestCircuitBreakers:
    @pytest.fixture(autouse=True)
    def reset_breakers(self):
        from src.backend.controller.worker.utils import indexing_breaker, parsing_breaker

        parsing_breaker.close()
        indexing_breaker.close()
        yield
        parsing_breaker.close()
        indexing_breaker.close()

    def test_parsing_breaker_opens_after_fail_max_failures(self) -> None:
        """After fail_max=3 failures the breaker opens and rejects without an HTTP call."""
        with respx.mock:
            route = respx.post(f"{_PARSING_URL}/v1/parse").mock(return_value=Response(503))
            for _ in range(3):
                with pytest.raises(Exception):
                    _parse()
            assert route.call_count == 3

            # 4th call — breaker OPEN: raises immediately, no HTTP request
            with pytest.raises(pybreaker.CircuitBreakerError):
                _parse()
            assert route.call_count == 3

    def test_indexing_breaker_opens_after_fail_max_failures(self) -> None:
        """Indexing breaker mirrors parsing breaker behaviour."""
        with respx.mock:
            route = respx.post(f"{_INDEXING_URL}/ingest").mock(return_value=Response(503))
            for _ in range(3):
                with pytest.raises(Exception):
                    _index()
            assert route.call_count == 3

            with pytest.raises(pybreaker.CircuitBreakerError):
                _index()
            assert route.call_count == 3

    def test_success_resets_failure_count(self) -> None:
        """Two failures then a success resets the counter; breaker stays CLOSED."""
        with respx.mock:
            respx.post(f"{_PARSING_URL}/v1/parse").mock(
                side_effect=[
                    Response(503),
                    Response(503),
                    Response(200, json=_SAMPLE_CHUNKS_JSON),
                    Response(503),
                ]
            )
            for _ in range(2):
                with pytest.raises(Exception):
                    _parse()

            _parse()  # success — counter resets to 0

            with pytest.raises(Exception):  # 1 failure, not 3 → still CLOSED
                _parse()

            assert parsing_breaker.current_state == "closed"

    def test_state_transition_logged_on_open(self, caplog) -> None:
        """Opening the circuit must emit a WARNING log with the state transition."""
        with caplog.at_level(logging.WARNING, logger="src.backend.controller.worker.utils"):
            with respx.mock:
                respx.post(f"{_PARSING_URL}/v1/parse").mock(return_value=Response(503))
                for _ in range(3):
                    with pytest.raises(Exception):
                        _parse()

        assert any("closed->open" in record.message for record in caplog.records)
