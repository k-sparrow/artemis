"""Unit tests for controller worker utility functions.

The service calls are claim-check: ``call_parsing_service`` sends a ``source_ref``
and returns the artifact ``BlobRef``; ``call_indexing_service`` sends an
``artifact_ref``. All HTTP is mocked with respx — no infrastructure required.
"""

from __future__ import annotations

import json
import logging
import uuid
from unittest.mock import MagicMock
from urllib.parse import parse_qs

import pybreaker
import pytest
import respx
from httpx import Response

from src.backend.controller.lib.schemas import BlobRef, S3Details, SourceDetails
from src.backend.controller.worker.utils import (
    call_indexing_service,
    call_parsing_service,
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
# call_parsing_service (source_ref in → artifact BlobRef out)
# ---------------------------------------------------------------------------


class TestCallParsingService:
    @respx.mock
    def test_happy_path_returns_artifact_ref(self) -> None:
        respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(
                200, json={"bucket": "parsed-chunks", "key": "parse/x.json"}
            )
        )

        artifact = call_parsing_service(
            source_ref=_SOURCE_REF,
            source=_source(),
            parsing_url=_PARSING_URL,
            timeout=5.0,
            logger=_logger,
        )

        assert isinstance(artifact, BlobRef)
        assert artifact.bucket == "parsed-chunks"
        assert artifact.key == "parse/x.json"

    @respx.mock
    def test_source_ref_filename_content_type_and_obj_id_sent_as_form(self) -> None:
        route = respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(200, json={"bucket": "b", "key": "k"})
        )

        call_parsing_service(
            source_ref=_SOURCE_REF,
            source=_source("report.pdf", "application/pdf"),
            parsing_url=_PARSING_URL,
            timeout=5.0,
            logger=_logger,
        )

        form = parse_qs(route.calls.last.request.content.decode())
        assert json.loads(form["source_ref"][0]) == {
            "bucket": _SOURCE_REF.bucket,
            "key": _SOURCE_REF.key,
        }
        assert form["filename"][0] == "report.pdf"
        assert form["content_type"][0] == "application/pdf"
        assert json.loads(form["metadata"][0])["obj_id"] == str(_OBJ_ID)

    @respx.mock
    def test_non_200_raises_http_status_error(self) -> None:
        respx.post(f"{_PARSING_URL}/v1/parse").mock(
            return_value=Response(503, json={"detail": "docling down"})
        )

        with pytest.raises(Exception):
            call_parsing_service(
                source_ref=_SOURCE_REF,
                source=_source(),
                parsing_url=_PARSING_URL,
                timeout=5.0,
                logger=_logger,
            )


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


# ---------------------------------------------------------------------------
# Circuit breakers
# ---------------------------------------------------------------------------


def _parse(url: str = _PARSING_URL) -> None:
    call_parsing_service(
        source_ref=_SOURCE_REF,
        source=_source(),
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
            route = respx.post(f"{_PARSING_URL}/v1/parse").mock(
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
            respx.post(f"{_PARSING_URL}/v1/parse").mock(
                side_effect=[
                    Response(503),
                    Response(503),
                    Response(200, json={"bucket": "b", "key": "k"}),
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
        with caplog.at_level(
            logging.WARNING, logger="src.backend.controller.worker.utils"
        ):
            with respx.mock:
                respx.post(f"{_PARSING_URL}/v1/parse").mock(return_value=Response(503))
                for _ in range(3):
                    with pytest.raises(Exception):
                        _parse()

        assert any("closed->open" in record.message for record in caplog.records)
