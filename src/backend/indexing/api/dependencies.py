from typing import AsyncIterator, Annotated, Callable

from fastapi import Depends
from langchain_core.document_loaders import BaseLoader
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from qdrant_client import AsyncQdrantClient

from src.backend.indexing.api.config import settings
from src.lib.core.adapters.loaders import (
    DoclingAPIServeLoader,
    DoclingServeAPIDocumentConverter,
    DEFAULT_CONVERTER_KWARGS,
)
from src.backend.indexing.lib.handler import (
    QdrantVectorStoreHandler,
    VectorStoreHandler,
)
from src.lib.core.ingestion import (
    BasePipeline,
    PipelineConfig,
    PipelineResources,
    PipelineType,
    create_pipeline,
)
from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings

__all__ = [
    "LoaderFactory",
    "PipelineType",
    "loader_factory_dependency",
    "pipeline_dependency",
    "vectorstore_dependency",
    "get_vectorstore_handler_solved",
]

LoaderFactory = Callable[[bytes, str, str], BaseLoader]


async def get_embeddings() -> Embeddings:
    return HuggingFaceEndpointEmbeddings(model=settings.TEI_HOST_URL)


def get_embeddings_sync() -> Embeddings:
    return HuggingFaceEndpointEmbeddings(model=settings.TEI_HOST_URL)


embeddings_dependency = Annotated[
    Embeddings,
    Depends(get_embeddings),
]


async def get_vectorstore_handler(
    embeddings: embeddings_dependency,
) -> VectorStoreHandler:
    return QdrantVectorStoreHandler(
        embeddings=embeddings,
        collection_name=settings.QDRANT_COLLECTION_NAME,
        base_url=settings.QDRANT_HOST_URI,
        eager=False,
    )


def get_async_qdrant_client() -> AsyncQdrantClient:
    return AsyncQdrantClient(url=settings.QDRANT_HOST_URI, prefer_grpc=False)


# an ugly patch for lifespan
async def get_vectorstore_handler_solved() -> VectorStoreHandler:
    return await get_vectorstore_handler(embeddings=await get_embeddings())


vectorstore_handler_dependency = Annotated[
    VectorStoreHandler,
    Depends(get_vectorstore_handler),
]


async def get_vectorstore(
    handler: vectorstore_handler_dependency,
) -> AsyncIterator[VectorStore]:
    try:
        yield handler.vectorstore
    finally:
        await handler.aclose()


vectorstore_dependency = Annotated[
    VectorStore,
    Depends(get_vectorstore),
]


async def get_pipeline(
    pipeline_type: PipelineType,
    vectorstore: vectorstore_dependency,
) -> BasePipeline:
    config = PipelineConfig(pipeline_type=pipeline_type)
    resources = PipelineResources(vectorstore=vectorstore)
    return create_pipeline(config=config, resources=resources)


pipeline_dependency = Annotated[
    BasePipeline,
    Depends(get_pipeline),
]


def get_loader_factory() -> LoaderFactory:
    """Returns a factory that creates document loaders configured for this service."""

    def factory(file: bytes, filename: str, content_type: str) -> BaseLoader:
        return DoclingAPIServeLoader(
            source=(filename, file, content_type),
            converter=DoclingServeAPIDocumentConverter(
                base_url=settings.DOCLING_SERVE_URI,
                **DEFAULT_CONVERTER_KWARGS,
            ),
        )

    return factory


loader_factory_dependency = Annotated[
    LoaderFactory,
    Depends(get_loader_factory),
]
