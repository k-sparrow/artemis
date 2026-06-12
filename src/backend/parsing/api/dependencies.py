from typing import Annotated

from fastapi import Depends
from minio import Minio

from src.lib.core.adapters.loaders import (
    LoaderConfig,
    LoaderFactory,
    LoaderType,
    create_loader_factory,
)
from src.lib.core.adapters.stores.base.blob import BlobStoreFactory
from src.lib.core.adapters.stores.minio.blob import MinioBlobStore
from src.backend.parsing.api.config import settings

__all__ = [
    "LoaderFactory",
    "loader_factory_dependency",
    "get_s3_client",
    "get_blob_store_factory",
    "blob_store_factory_dependency",
]


def get_loader_factory(
    loader_type: LoaderType = settings.LOADER_TYPE,
) -> LoaderFactory:
    config = LoaderConfig(
        loader_type=loader_type,
        docling_base_url=settings.DOCLING_SERVE_URI,
    )
    return create_loader_factory(config)


def get_s3_client() -> Minio:
    return Minio(
        endpoint=settings.S3_ENDPOINT,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=settings.S3_SECURE,
    )


def get_blob_store_factory(
    client: Annotated[Minio, Depends(get_s3_client)],
) -> BlobStoreFactory:
    """Yield a ``(bucket) -> BlobStore`` factory used for both reading the input
    (``source_ref.bucket``) and writing the artifact. One overridable seam for
    both claim-check directions."""
    return lambda bucket: MinioBlobStore(client, bucket)


loader_factory_dependency = Annotated[LoaderFactory, Depends(get_loader_factory)]
blob_store_factory_dependency = Annotated[
    BlobStoreFactory, Depends(get_blob_store_factory)
]
