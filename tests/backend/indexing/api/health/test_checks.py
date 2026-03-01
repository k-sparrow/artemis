import pytest

from src.backend.indexing.api.health.checks import LivenessCheck, QdrantHealthcheck
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
        assert result.name == "Qdrant"
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
        assert result.name == "Qdrant"
        assert "Missing 'metadata.namespace' index" in result.details

    @pytest.mark.asyncio
    async def test_namespace_index_not_tenant(self, qdrant_client_with_index_no_tenant):
        """Should fail when namespace index exists but is not configured as tenant."""
        check = QdrantHealthcheck(
            client=qdrant_client_with_index_no_tenant,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is False
        assert result.name == "Qdrant"
        assert "not configured as tenant" in result.details

    @pytest.mark.asyncio
    async def test_proper_tenant_index_passes(self, qdrant_client_with_tenant_index):
        """Should pass when collection has proper tenant partition index."""
        check = QdrantHealthcheck(
            client=qdrant_client_with_tenant_index,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is True
        assert result.name == "Qdrant"

    @pytest.mark.asyncio
    async def test_connection_error_fails_gracefully(self):
        """Should fail gracefully when Qdrant connection fails."""
        # Create a client pointing to non-existent server
        # Using a closed client to simulate connection error
        from qdrant_client import QdrantClient

        client = QdrantClient(":memory:")
        client.close()

        check = QdrantHealthcheck(
            client=client,
            collection_name=COLLECTION_NAME,
        )
        result = await check()

        assert result.passed is False
        assert result.name == "Qdrant"
        assert "connection error" in result.details.lower()
