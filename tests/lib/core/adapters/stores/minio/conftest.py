"""Session-scoped MinIO testcontainer for Tier 2 integration tests."""
import pytest
from minio import Minio
from testcontainers.minio import MinioContainer

_MINIO_IMAGE = "minio/minio:latest"
_ACCESS_KEY = "minioadmin"
_SECRET_KEY = "minioadmin"


@pytest.fixture(scope="session")
def minio_container(request: pytest.FixtureRequest) -> MinioContainer:
    container = MinioContainer(
        image=_MINIO_IMAGE,
        access_key=_ACCESS_KEY,
        secret_key=_SECRET_KEY,
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def minio_client(minio_container: MinioContainer) -> Minio:
    return minio_container.get_client()