import httpx
import pytest
import respx
from qdrant_client import QdrantClient

from src.backend.indexing.api.health.checks import (
    LivenessCheck,
    OpenAICompatibleHealthcheck,
    QdrantHealthcheck,
    TeiHealthcheck,
)
from tests.backend.indexing.api.health.conftest import COLLECTION_NAME


class TestLivenessCheck:
    """Tests for the LivenessCheck class."""

    @pytest.mark.asyncio
    async def test_liveness_check_always_passes(self):
        """Liveness check should always return passed=True."""
        check = LivenessCheck()
        result = await check()

        assert result.passed is True
        assert result.name == "Liveness"


class TestQdrantHealthcheck:
    """Tests for the QdrantHealthcheck class."""

    @pytest.mark.asyncio
    async def test_collection_does_not_exist(self, qdrant_client):
        """Should fail when collection doesn't exist."""
        check = QdrantHealthcheck(
            client=qdrant_client,
            collection_name="nonexistent_collection",
        )
        result = await check()

        assert result.passed is False
        assert result.name == "Readiness/Qdrant"
        assert "does not exist" in result.details

    @pytest.mark.asyncio
    async def test_collection_exists_but_no_namespace_index(
        self, qdrant_client_with_collection
    ):
        """Should fail when collection exists but has no namespace index."""
        check = QdrantHealthcheck(
            client=qdrant_client_with_collection,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is False
        assert result.name == "Readiness/Qdrant"
        assert "Missing 'metadata.namespace_id' index" in result.details

    @pytest.mark.asyncio
    async def test_namespace_index_not_tenant(self, qdrant_client_with_index_no_tenant):
        """Should fail when namespace index exists but is not configured as tenant."""
        check = QdrantHealthcheck(
            client=qdrant_client_with_index_no_tenant,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is False
        assert result.name == "Readiness/Qdrant"
        assert (
            "'metadata.namespace_id' index has no params configured" in result.details
        )

    @pytest.mark.asyncio
    async def test_proper_tenant_index_passes(self, qdrant_client_with_tenant_index):
        """Should pass when collection has proper tenant partition index."""
        check = QdrantHealthcheck(
            client=qdrant_client_with_tenant_index,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is True
        assert result.name == "Readiness/Qdrant"

    @pytest.mark.asyncio
    async def test_connection_error_fails_gracefully(self, closed_client: QdrantClient):
        """Should fail gracefully when Qdrant connection fails."""
        # Create a client pointing to non-existent server
        # Using a closed client to simulate connection error

        check = QdrantHealthcheck(
            client=closed_client,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is False
        assert result.name == "Readiness/Qdrant"
        assert "connection error" in result.details.lower()


@pytest.mark.unit
class TestTeiHealthcheck:
    _TEI_URL = "http://tei-host:8080"

    @respx.mock
    @pytest.mark.asyncio
    async def test_embedding_model_ready(self):
        respx.get(f"{self._TEI_URL}/info").mock(
            return_value=httpx.Response(
                200,
                json={
                    "model_id": "Alibaba-NLP/gte-large-en-v1.5",
                    "model_type": {"embedding": {"pooling": "mean"}},
                },
            )
        )
        check = TeiHealthcheck(tei_url=self._TEI_URL)
        result = await check()

        assert result.passed is True
        assert result.name == "Readiness/TEI"
        assert "gte-large-en-v1.5" in result.details

    @respx.mock
    @pytest.mark.asyncio
    async def test_non_embedding_model_fails(self):
        respx.get(f"{self._TEI_URL}/info").mock(
            return_value=httpx.Response(
                200,
                json={
                    "model_id": "some-reranker",
                    "model_type": {"reranker": {}},
                },
            )
        )
        check = TeiHealthcheck(tei_url=self._TEI_URL)
        result = await check()

        assert result.passed is False
        assert "not serving an embedding model" in result.details

    @respx.mock
    @pytest.mark.asyncio
    async def test_http_error_fails_gracefully(self):
        respx.get(f"{self._TEI_URL}/info").mock(
            return_value=httpx.Response(503)
        )
        check = TeiHealthcheck(tei_url=self._TEI_URL)
        result = await check()

        assert result.passed is False
        assert result.name == "Readiness/TEI"

    @respx.mock
    @pytest.mark.asyncio
    async def test_connection_error_fails_gracefully(self):
        respx.get(f"{self._TEI_URL}/info").mock(
            side_effect=httpx.ConnectError("refused")
        )
        check = TeiHealthcheck(tei_url=self._TEI_URL)
        result = await check()

        assert result.passed is False
        assert "connection error" in result.details.lower()


@pytest.mark.unit
class TestOpenAICompatibleHealthcheck:
    _URL = "http://colbert-host:8080"
    _MODEL = "jinaai/jina-colbert-v2"
    _NAME = "Readiness/ColBERT"

    @respx.mock
    @pytest.mark.asyncio
    async def test_model_present_passes(self):
        respx.get(f"{self._URL}/v1/models").mock(
            return_value=httpx.Response(
                200,
                json={"data": [{"id": self._MODEL}, {"id": "other-model"}]},
            )
        )
        check = OpenAICompatibleHealthcheck(
            url=self._URL, model_name=self._MODEL, name=self._NAME
        )
        result = await check()

        assert result.passed is True
        assert result.name == self._NAME
        assert self._MODEL in result.details

    @respx.mock
    @pytest.mark.asyncio
    async def test_model_not_in_list_fails(self):
        respx.get(f"{self._URL}/v1/models").mock(
            return_value=httpx.Response(
                200,
                json={"data": [{"id": "some-other-model"}]},
            )
        )
        check = OpenAICompatibleHealthcheck(
            url=self._URL, model_name=self._MODEL, name=self._NAME
        )
        result = await check()

        assert result.passed is False
        assert self._MODEL in result.details
        assert "not found" in result.details

    @respx.mock
    @pytest.mark.asyncio
    async def test_http_error_fails_gracefully(self):
        respx.get(f"{self._URL}/v1/models").mock(
            return_value=httpx.Response(503)
        )
        check = OpenAICompatibleHealthcheck(
            url=self._URL, model_name=self._MODEL, name=self._NAME
        )
        result = await check()

        assert result.passed is False
        assert result.name == self._NAME

    @respx.mock
    @pytest.mark.asyncio
    async def test_connection_error_fails_gracefully(self):
        respx.get(f"{self._URL}/v1/models").mock(
            side_effect=httpx.ConnectError("refused")
        )
        check = OpenAICompatibleHealthcheck(
            url=self._URL, model_name=self._MODEL, name=self._NAME
        )
        result = await check()

        assert result.passed is False
        assert "connection error" in result.details.lower()
