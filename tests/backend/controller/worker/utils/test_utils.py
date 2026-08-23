"""Unit tests for controller worker utility functions.

``call_indexing_service`` sends an ``artifact_ref`` and returns the ingestion
result. All HTTP is mocked with respx — no infrastructure required.
"""

from __future__ import annotations

import json
import logging
import uuid
from unittest.mock import MagicMock

import httpx
import pybreaker
import pytest
import respx
from httpx import Response

from src.backend.controller.lib.schemas import BlobRef, S3Details, SourceDetails
from src.backend.controller.worker.utils import (
    call_indexing_service,
    call_parse_status,
    fetch_from_s3,
    parsing_breaker,
)

_logger = logging.getLogger(__name__)

_PARSING_URL = "http://test-parsing:10001"
_INDEXING_URL = "http://test-indexing:10000"

_OBJ_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")
_SOURCE_REF = BlobRef(bucket="artemis", key="artemis/in.md")
_ARTIFACT_REF = BlobRef(bucket="parsed-chunks", key=f"parse/{_OBJ_ID}.json")


def _source(
    filename: str = "test.md", content_type: str = "text/markdown"
) -> SourceDetails:
    return SourceDetails(
        source=filename, content_type=content_type, obj_id=_OBJ_ID, object_type="file"
    )


# ---------------------------------------------------------------------------
# fetch_from_s3 (still used as a generic util)
# ---------------------------------------------------------------------------


class TestFetchFromS3:
    def _s3(self) -> S3Details:
        return S3Details(bucket="my-bucket", object="docs/test.pdf", size=100)

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
# call_indexing_service (artifact_ref in → result dict out)
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
            artifact_ref=_ARTIFACT_REF,
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
            artifact_ref=_ARTIFACT_REF,
            namespace_id=self._NAMESPACE,
            ingestion_url=_INDEXING_URL,
            timeout=5.0,
            logger=_logger,
        )

        assert str(self._NAMESPACE) in str(route.calls.last.request.url)

    @respx.mock
    def test_artifact_ref_sent_as_json_body(self) -> None:
        route = respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(200, json=self._RESULT)
        )

        call_indexing_service(
            artifact_ref=_ARTIFACT_REF,
            namespace_id=self._NAMESPACE,
            ingestion_url=_INDEXING_URL,
            timeout=5.0,
            logger=_logger,
        )

        body = json.loads(route.calls.last.request.content)
        assert body == {
            "artifact_ref": {"bucket": _ARTIFACT_REF.bucket, "key": _ARTIFACT_REF.key}
        }

    @respx.mock
    def test_non_200_raises(self) -> None:
        respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(422, json={"detail": "bad artifact"})
        )

        with pytest.raises(Exception):
            call_indexing_service(
                artifact_ref=_ARTIFACT_REF,
                namespace_id=self._NAMESPACE,
                ingestion_url=_INDEXING_URL,
                timeout=5.0,
                logger=_logger,
            )

    @respx.mock
    def test_4xx_detail_preserved_in_exception_message(self) -> None:
        """httpx.HTTPStatusError's own str() is status/URL only — the indexing
        service's actual detail (what OutboxTask.on_failure ends up storing as
        ingestion_status.failure_reason) must survive into the raised
        exception's message, and the raised exception must still be an
        httpx.HTTPStatusError so the index task's 5xx-retry/4xx-permanent
        branching (isinstance check) keeps working."""
        respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(
                422,
                json={
                    "detail": "chunking produced zero chunks for obj_id=...",
                    "type": "document_processing_error",
                },
            )
        )

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            call_indexing_service(
                artifact_ref=_ARTIFACT_REF,
                namespace_id=self._NAMESPACE,
                ingestion_url=_INDEXING_URL,
                timeout=5.0,
                logger=_logger,
            )

        assert "chunking produced zero chunks" in str(exc_info.value)
        assert exc_info.value.response.status_code == 422

    @respx.mock
    def test_non_json_error_body_still_raises_original(self) -> None:
        """A 4xx with a non-JSON body must not blow up detail-extraction — the
        original httpx.HTTPStatusError still propagates."""
        respx.post(f"{_INDEXING_URL}/ingest").mock(
            return_value=Response(500, text="upstream gateway error")
        )

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            call_indexing_service(
                artifact_ref=_ARTIFACT_REF,
                namespace_id=self._NAMESPACE,
                ingestion_url=_INDEXING_URL,
                timeout=5.0,
                logger=_logger,
            )

        assert exc_info.value.response.status_code == 500


# ---------------------------------------------------------------------------
# Circuit breakers
# ---------------------------------------------------------------------------


def _parse(url: str = _PARSING_URL) -> None:
    call_parse_status(
        task_id="stub-task-id",
        parsing_url=url,
        timeout=5.0,
        logger=_logger,
    )


def _index(url: str = _INDEXING_URL) -> None:
    call_indexing_service(
        artifact_ref=_ARTIFACT_REF,
        namespace_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        ingestion_url=url,
        timeout=5.0,
        logger=_logger,
    )


class TestCircuitBreakers:
    @pytest.fixture(autouse=True)
    def reset_breakers(self):
        from src.backend.controller.worker.utils import (
            indexing_breaker,
            parsing_breaker,
        )

        parsing_breaker.close()
        indexing_breaker.close()
        yield
        parsing_breaker.close()
        indexing_breaker.close()

    def test_parsing_breaker_opens_after_fail_max_failures(self) -> None:
        with respx.mock:
            route = respx.get(f"{_PARSING_URL}/v1/parse/status/stub-task-id").mock(
                return_value=Response(503)
            )
            for _ in range(3):
                with pytest.raises(Exception):
                    _parse()
            assert route.call_count == 3

            with pytest.raises(pybreaker.CircuitBreakerError):
                _parse()
            assert route.call_count == 3

    def test_indexing_breaker_opens_after_fail_max_failures(self) -> None:
        with respx.mock:
            route = respx.post(f"{_INDEXING_URL}/ingest").mock(
                return_value=Response(503)
            )
            for _ in range(3):
                with pytest.raises(Exception):
                    _index()
            assert route.call_count == 3

            with pytest.raises(pybreaker.CircuitBreakerError):
                _index()
            assert route.call_count == 3

    def test_success_resets_failure_count(self) -> None:
        with respx.mock:
            respx.get(f"{_PARSING_URL}/v1/parse/status/stub-task-id").mock(
                side_effect=[
                    Response(503),
                    Response(503),
                    Response(
                        200,
                        json={
                            "status": "success",
                            "num_processed": 1,
                            "num_total": 1,
                            "error_message": None,
                        },
                    ),
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

    def test_429_does_not_open_breaker(self) -> None:
        """docling-serve queue-full backpressure (429) is expected, not a
        system failure — it must not count toward fail_max, or the breaker
        would trip on healthy backpressure and override the dedicated
        429-retry policy in submit_parse/submit_chunk with a much shorter
        (~45min) breaker-recovery budget."""
        with respx.mock:
            respx.get(f"{_PARSING_URL}/v1/parse/status/stub-task-id").mock(
                return_value=Response(429)
            )
            for _ in range(5):  # well past fail_max=3
                with pytest.raises(httpx.HTTPStatusError):
                    _parse()

            assert parsing_breaker.current_state == "closed"

    def test_state_transition_logged_on_open(self, caplog) -> None:
        with caplog.at_level(
            logging.WARNING, logger="src.backend.controller.worker.utils"
        ):
            with respx.mock:
                respx.get(f"{_PARSING_URL}/v1/parse/status/stub-task-id").mock(
                    return_value=Response(503)
                )
                for _ in range(3):
                    with pytest.raises(Exception):
                        _parse()

        assert any("closed->open" in record.message for record in caplog.records)
