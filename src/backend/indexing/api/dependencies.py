from dataclasses import dataclass
from typing import AsyncIterator, Annotated, Type, Union
from uuid import UUID

from fastapi import Depends
from langchain.indexes import SQLRecordManager
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from minio import Minio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from qdrant_client import AsyncQdrantClient

from src.lib.core.adapters.stores.base import MetadataKeyedDocumentIndex
from src.lib.core.adapters.stores.base.blob import BlobStoreFactory
from src.lib.core.adapters.stores.minio.blob import MinioBlobStore
from src.lib.core.adapters.stores.s3 import S3ByteStore
from src.backend.indexing.api.config import settings
from src.lib.core.adapters.embedding.huggingface import HuggingFaceEndpointEmbeddings
from src.lib.core.adapters.embedding.late_interaction import (
    VLLMLateInteractionEmbeddings,
)
from src.lib.core.adapters.embedding.sparse import FastEmbedSparseEmbeddings
from src.lib.core.adapters.stores.sql.store import SQLDocumentIndex
from src.lib.core.adapters.vectorstore.qdrant import RetrievalMode
from src.backend.indexing.lib.handler import (
    QdrantVectorStoreHandler,
    VectorStoreHandler,
)
from src.backend.indexing.lib.handler.config import (
    MULTI_TENANT_DENSE,
    MULTI_TENANT_HYBRID,
    MULTI_TENANT_MULTI_STAGE,
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
from src.lib.core.ingestion.pipeline.parent_page import ParentPagePipeline

__all__ = [
    "pipeline_dependency",
    "vectorstore_dependency",
    "get_vectorstore_handler_solved",
    "get_s3_client",
    "get_blob_store_factory",
    "blob_store_factory_dependency",
    "build_page_byte_store",
]


def get_s3_client() -> Minio:
    return Minio(
        endpoint=settings.S3_ENDPOINT,
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        secure=settings.S3_SECURE,
    )


def build_page_byte_store() -> S3ByteStore:
    """Construct the parent-page doc store (aiobotocore, async-native).

    Shared by the lifespan (which calls ``ensure_bucket``) and the pipeline /
    retriever wiring. The store object is cheap — the S3 client is created
    per-operation — so building one per request is fine; ``S3ByteStore`` speaks
    plain S3, so the same ``S3_*`` settings reach MinIO here as elsewhere.
    """
    scheme = "https" if settings.S3_SECURE else "http"
    return S3ByteStore(
        endpoint_url=f"{scheme}://{settings.S3_ENDPOINT}",
        access_key=settings.S3_ACCESS_KEY,
        secret_key=settings.S3_SECRET_KEY,
        bucket=settings.PAGE_BUCKET,
    )


def get_blob_store_factory(
    client: Annotated[Minio, Depends(get_s3_client)],
) -> BlobStoreFactory:
    """Yield a ``(bucket) -> BlobStore`` factory. The artifact's bucket travels in
    the BlobRef, so the reader is built per-request from ``ref.bucket``. One
    overridable seam — a unit test swaps in a fake."""
    return lambda bucket: MinioBlobStore(client, bucket)


blob_store_factory_dependency = Annotated[
    BlobStoreFactory, Depends(get_blob_store_factory)
]

# FastEmbedSparseEmbeddings loads the Qdrant/bm25 model on instantiation.
# The model is stateless (corpus statistics live in Qdrant, not here),
# so one instance is shared across all requests.
_sparse_embeddings = (
    FastEmbedSparseEmbeddings()
    if settings.RETRIEVAL_MODE in ("hybrid", "multi_stage")
    else None
)

# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------

_INDEXER_CONFIG_CLS: dict[
    PipelineType,
    Type[Union[SimpleIndexerConfig, SemiStructuredIndexerConfig]],
] = {
    PipelineType.SIMPLE: SimpleIndexerConfig,
    PipelineType.SEMI_STRUCTURED: SemiStructuredIndexerConfig,
}


@dataclass(frozen=True)
class ResourceConfig:
    """Derives all storage namespace strings from a single *namespace* token."""

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

    @property
    def page_rm_namespace(self) -> str:
        """Record-manager namespace for the parent-page doc store.

        Distinct from the vector-store RM so parent-page diffing (page content
        hashes) never collides with chunk diffing in the same namespace.
        """
        return f"{self.vectorstore_rm_namespace}:pages"


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
    mode = settings.RETRIEVAL_MODE
    if mode == RetrievalMode.MULTI_STAGE:
        if not settings.COLBERT_HOST_URL:
            raise RuntimeError(
                "COLBERT_HOST_URL must be set when RETRIEVAL_MODE=multi_stage"
            )
        return QdrantVectorStoreHandler(
            embeddings=embeddings,
            collection_name=settings.QDRANT_COLLECTION_NAME,
            base_url=settings.QDRANT_HOST_URI,
            collection_config=MULTI_TENANT_MULTI_STAGE,
            retrieval_mode=mode,
            sparse_embedding=_sparse_embeddings,
            late_interaction_embedding=VLLMLateInteractionEmbeddings(
                url=settings.COLBERT_HOST_URL,
                model=settings.COLBERT_MODEL_NAME,
            ),
            eager=False,
        )
    if mode == RetrievalMode.HYBRID:
        return QdrantVectorStoreHandler(
            embeddings=embeddings,
            collection_name=settings.QDRANT_COLLECTION_NAME,
            base_url=settings.QDRANT_HOST_URI,
            collection_config=MULTI_TENANT_HYBRID,
            retrieval_mode=mode,
            sparse_embedding=_sparse_embeddings,
            eager=False,
        )
    return QdrantVectorStoreHandler(
        embeddings=embeddings,
        collection_name=settings.QDRANT_COLLECTION_NAME,
        base_url=settings.QDRANT_HOST_URI,
        collection_config=MULTI_TENANT_DENSE,
        retrieval_mode=mode,
        eager=False,
    )


def get_async_qdrant_client() -> AsyncQdrantClient:
    return AsyncQdrantClient(url=settings.QDRANT_HOST_URI, prefer_grpc=False)


qdrant_client_dependency = Annotated[
    AsyncQdrantClient,
    Depends(get_async_qdrant_client),
]


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
    inner = create_pipeline(config=pipeline_cfg, resources=resources)

    # Parent-page caching is ALWAYS ON, composed over whichever algorithm `inner`
    # is (Simple / SemiStructured / …) — the inner pipeline stays untouched, the
    # way a reranker composes over search. Every object's pages are cached and
    # every chunk gets a ``parent_id``, so retrieval can opt into parent-page
    # dereference per request with no re-embed. The page record manager keeps
    # lc-index's free diffing; the doc store is keyed deterministically by
    # ``page_key`` (see ingestion/RECORD_MANAGER.md §9).
    byte_store = build_page_byte_store()
    page_record_manager = SQLRecordManager(
        namespace=resource_cfg.page_rm_namespace,
        engine=engine,
        async_mode=True,
    )
    await page_record_manager.acreate_schema()
    return ParentPagePipeline(
        inner,
        record_manager=page_record_manager,
        document_index=MetadataKeyedDocumentIndex(store=byte_store),
        byte_store=byte_store,
        namespace_id=str(namespace),
    )


pipeline_dependency = Annotated[
    BasePipeline,
    Depends(get_pipeline),
]
