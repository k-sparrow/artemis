import pytest
from qdrant_client import AsyncQdrantClient, QdrantClient, models
from testcontainers.qdrant import QdrantContainer

COLLECTION_NAME = "test_collection"
_QDRANT_IMAGE = "qdrant/qdrant:v1.17"


@pytest.fixture(scope="session")
def qdrant_container(request: pytest.FixtureRequest) -> QdrantContainer:
    container = QdrantContainer(_QDRANT_IMAGE)
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture
def qdrant_client(
    qdrant_container: QdrantContainer, request: pytest.FixtureRequest
) -> AsyncQdrantClient:
    """AsyncQdrantClient with no collection — for 'collection does not exist' tests."""
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))

    sync_client = QdrantClient(host=host, port=port, prefer_grpc=False)
    if sync_client.collection_exists(COLLECTION_NAME):
        sync_client.delete_collection(COLLECTION_NAME)
    sync_client.close()

    return AsyncQdrantClient(host=host, port=port, prefer_grpc=False)


@pytest.fixture
def qdrant_client_with_collection(
    qdrant_container: QdrantContainer, request: pytest.FixtureRequest
) -> AsyncQdrantClient:
    """AsyncQdrantClient with a bare collection and no payload index."""
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))

    sync_client = QdrantClient(host=host, port=port, prefer_grpc=False)
    sync_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE),
    )

    def _teardown() -> None:
        sync_client.delete_collection(COLLECTION_NAME)
        sync_client.close()

    request.addfinalizer(_teardown)
    return AsyncQdrantClient(host=host, port=port, prefer_grpc=False)


@pytest.fixture
def qdrant_client_with_index_no_tenant(
    qdrant_container: QdrantContainer, request: pytest.FixtureRequest
) -> AsyncQdrantClient:
    """AsyncQdrantClient with collection and namespace index, but NOT as tenant."""
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))

    sync_client = QdrantClient(host=host, port=port, prefer_grpc=False)
    sync_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE),
    )
    sync_client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="metadata.namespace_id",
        field_schema=models.PayloadSchemaType.KEYWORD,
    )

    def _teardown() -> None:
        sync_client.delete_collection(COLLECTION_NAME)
        sync_client.close()

    request.addfinalizer(_teardown)
    return AsyncQdrantClient(host=host, port=port, prefer_grpc=False)


@pytest.fixture
def qdrant_client_with_tenant_index(
    qdrant_container: QdrantContainer, request: pytest.FixtureRequest
) -> AsyncQdrantClient:
    """AsyncQdrantClient with collection and proper tenant partition index."""
    host = qdrant_container.get_container_host_ip()
    port = int(qdrant_container.get_exposed_port(6333))

    sync_client = QdrantClient(host=host, port=port, prefer_grpc=False)
    sync_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE),
    )
    sync_client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="metadata.namespace_id",
        field_schema=models.UuidIndexParams(
            type=models.UuidIndexType.UUID,
            is_tenant=True,
        ),
    )

    def _teardown() -> None:
        sync_client.delete_collection(COLLECTION_NAME)
        sync_client.close()

    request.addfinalizer(_teardown)
    return AsyncQdrantClient(host=host, port=port, prefer_grpc=False)


@pytest.fixture()
def closed_client() -> QdrantClient:
    client = QdrantClient(":memory:")
    client.close()
