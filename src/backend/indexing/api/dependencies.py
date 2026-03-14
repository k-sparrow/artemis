from dataclasses import dataclass
from typing import AsyncIterator, Annotated, Type, Union
from uuid import UUID

from fastapi import Depends
from langchain.indexes import SQLRecordManager
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from qdrant_client import AsyncQdrantClient

from src.backend.indexing.api.config import settings
from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings
from src.lib.core.adapters.loaders import (
    LoaderConfig,
    LoaderFactory,
    LoaderType,
    create_loader_factory,
)
from src.lib.core.adapters.stores.sql.store import SQLDocumentIndex
from src.backend.indexing.lib.handler import (
    QdrantVectorStoreHandler,
    VectorStoreHandler,
)
from src.lib.core.ingestion import (
    BasePipeline,
    PipelineConfig,
    PipelineResources,
    SemiStructuredResources,
    PipelineType,
    SimpleIndexerConfig,
    SemiStructuredIndexerConfig,
    create_pipeline,
)

__all__ = [
    "LoaderFactory",
    "loader_factory_dependency",
    "pipeline_dependency",
    "vectorstore_dependency",
    "get_vectorstore_handler_solved",
]

# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------

# Maps each pipeline type to its indexer config class.
# Both share chunk_size / chunk_overlap, so construction is uniform.
_INDEXER_CONFIG_CLS: dict[
    PipelineType,
    Type[Union[SimpleIndexerConfig, SemiStructuredIndexerConfig]],
] = {
    PipelineType.SIMPLE: SimpleIndexerConfig,
    PipelineType.SEMI_STRUCTURED: SemiStructuredIndexerConfig,
}


@dataclass(frozen=True)
class ResourceConfig:
    """Derives all storage namespace strings from a single *namespace* token.

    The *namespace* value is supplied per-request (e.g. a chat-id or project
    slug) and drives every storage key so that documents from different tenants
    remain fully isolated.

    Attributes:
        namespace: Tenant-level namespace token supplied by the caller.
    """

    namespace: UUID

    @property
    def vectorstore_rm_namespace(self) -> str:
        """Record-manager namespace for the vector store."""
        return f"qdrant/{self.namespace}"

    @property
    def docstore_namespace(self) -> str:
        """Namespace used to partition originals in the SQL docstore."""
        return f"docstore/{self.namespace}"

    @property
    def docstore_rm_namespace(self) -> str:
        """Record-manager namespace for the SQL docstore."""
        return f"docstore/{self.namespace}:originals"


async def acreate_resources(
    pipeline_config: PipelineConfig,
    resource_config: ResourceConfig,
    vectorstore: VectorStore,
    engine: AsyncEngine,
) -> PipelineResources:
    """Construct the correct :class:`PipelineResources` subclass for *pipeline_config*.

    Centralises the per-type branching so callers stay declarative.
    """
    record_manager = SQLRecordManager(
        namespace=resource_config.vectorstore_rm_namespace,
        engine=engine,
        async_mode=True,
    )
    await record_manager.acreate_schema()

    match pipeline_config.pipeline_type:
        case PipelineType.SIMPLE:
            return PipelineResources(
                vectorstore=vectorstore,
                record_manager=record_manager,
            )
        case PipelineType.SEMI_STRUCTURED:
            docstore_rm = SQLRecordManager(
                namespace=resource_config.docstore_rm_namespace,
                engine=engine,
                async_mode=True,
            )
            await docstore_rm.acreate_schema()
            document_index = SQLDocumentIndex.from_engine(
                namespace=resource_config.docstore_namespace, engine=engine
            )
            return SemiStructuredResources(
                vectorstore=vectorstore,
                record_manager=record_manager,
                document_index=document_index,
                docstore_record_manager=docstore_rm,
            )
        case _:
            raise ValueError(
                f"Unknown pipeline type: {pipeline_config.pipeline_type!r}"
            )


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------


async def get_embeddings() -> Embeddings:
    return HuggingFaceEndpointEmbeddings(model=settings.TEI_HOST_URL)


def get_embeddings_sync() -> Embeddings:
    return HuggingFaceEndpointEmbeddings(model=settings.TEI_HOST_URL)


embeddings_dependency = Annotated[
    Embeddings,
    Depends(get_embeddings),
]

# ---------------------------------------------------------------------------
# Vectorstore
# ---------------------------------------------------------------------------


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
        yield await handler.acreate()
    finally:
        await handler.aclose()


vectorstore_dependency = Annotated[
    VectorStore,
    Depends(get_vectorstore),
]

# ---------------------------------------------------------------------------
# SQL engine
# ---------------------------------------------------------------------------


def get_async_engine() -> AsyncEngine:
    return create_async_engine(settings.SQL_DB_URL)


async_engine_dependency = Annotated[
    AsyncEngine,
    Depends(get_async_engine),
]

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


async def get_pipeline(
    vectorstore: vectorstore_dependency,
    engine: async_engine_dependency,
    namespace: UUID,
    pipeline_type: PipelineType = settings.DEFAULT_PIPELINE_TYPE,
    chunk_size: int = settings.DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = settings.DEFAULT_CHUNK_OVERLAP,
) -> BasePipeline:
    indexer_cfg = _INDEXER_CONFIG_CLS[pipeline_type](
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    pipeline_cfg = PipelineConfig(pipeline_type=pipeline_type, indexer=indexer_cfg)
    resource_cfg = ResourceConfig(namespace=namespace)
    resources = await acreate_resources(pipeline_cfg, resource_cfg, vectorstore, engine)
    return create_pipeline(config=pipeline_cfg, resources=resources)


pipeline_dependency = Annotated[
    BasePipeline,
    Depends(get_pipeline),
]

# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def get_loader_factory(
    loader_type: LoaderType = LoaderType.DOCLING,
) -> LoaderFactory:
    config = LoaderConfig(
        loader_type=loader_type,
        docling_base_url=settings.DOCLING_SERVE_URI,
    )
    return create_loader_factory(config)


loader_factory_dependency = Annotated[
    LoaderFactory,
    Depends(get_loader_factory),
]
