import pytest
from qdrant_client import QdrantClient, models


COLLECTION_NAME = "test_collection"


@pytest.fixture
def qdrant_client():
    """Create an in-memory Qdrant client for testing."""
    client = QdrantClient(":memory:")
    yield client
    client.close()


@pytest.fixture
def qdrant_client_with_collection(qdrant_client):
    """Create an in-memory Qdrant client with a collection (no index)."""
    qdrant_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=384,
            distance=models.Distance.COSINE,
        ),
    )
    return qdrant_client


@pytest.fixture
def qdrant_client_with_index_no_tenant(qdrant_client_with_collection):
    """Create a Qdrant client with collection and namespace index but NOT as tenant."""
    qdrant_client_with_collection.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="metadata.namespace",
        field_schema=models.PayloadSchemaType.KEYWORD,
    )
    return qdrant_client_with_collection


@pytest.fixture
def qdrant_client_with_tenant_index(qdrant_client_with_collection):
    """Create a Qdrant client with collection and proper tenant partition index."""
    qdrant_client_with_collection.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="metadata.namespace",
        field_schema=models.UuidIndexParams(
            type=models.UuidIndexType.UUID,
            is_tenant=True,
        ),
    )
    return qdrant_client_with_collection
