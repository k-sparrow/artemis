from typing import AsyncIterator, Annotated

from fastapi import Depends
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from langchain_huggingface.embeddings import HuggingFaceEndpointEmbeddings

from src.backend.indexing.api.config import settings
from src.backend.indexing.lib.handler import (
    QdrantVectorStoreHandler,
    VectorStoreHandler,
)


__all__ = [
    "vectorstore_dependency",
    "get_vectorstore_handler_solved",
]


async def get_embeddings() -> Embeddings:
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
