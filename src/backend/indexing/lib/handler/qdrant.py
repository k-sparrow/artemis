from __future__ import annotations

from typing import TYPE_CHECKING, Any

from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from qdrant_client import AsyncQdrantClient, QdrantClient, models

from src.backend.indexing.lib.handler.base import BaseVectorHandler
from src.backend.indexing.lib.handler.config import (
    MULTI_TENANT_DENSE,
    QdrantCollectionConfig,
)
from src.backend.indexing.lib.vectorstore import AsyncQdrantVectorStore
from src.lib.core.adapters.vectorstore.qdrant import QdrantVectorStore, RetrievalMode

if TYPE_CHECKING:
    from langchain_qdrant.sparse_embeddings import SparseEmbeddings

__all__ = [
    "QdrantVectorStoreHandler",
]

_DENSE_VECTOR_NAME = QdrantVectorStore.VECTOR_NAME


class QdrantVectorStoreHandler(BaseVectorHandler):
    client: QdrantClient

    def __init__(
        self,
        embeddings: Embeddings,
        collection_name: str,
        base_url: str,
        collection_config: QdrantCollectionConfig = MULTI_TENANT_DENSE,
        retrieval_mode: RetrievalMode = RetrievalMode.DENSE,
        sparse_embedding: SparseEmbeddings | None = None,
        late_interaction_embedding: Any | None = None,
        eager: bool = False,
        **kwargs,
    ):
        self._collection_name = collection_name
        self._base_url = base_url
        self._collection_config = collection_config
        self._retrieval_mode = retrieval_mode
        self._sparse_embedding = sparse_embedding
        self._late_interaction_embedding = late_interaction_embedding
        self.client = None
        self.async_client = None
        super().__init__(embeddings=embeddings, eager=eager, **kwargs)

    async def acreate(self) -> VectorStore:
        self.client = QdrantClient(url=self._base_url, prefer_grpc=False)
        self.async_client = AsyncQdrantClient(url=self._base_url, prefer_grpc=False)

        if not self.client.collection_exists(self._collection_name):
            try:
                size = len(self.embeddings.embed_query("dummy"))
                self.client.create_collection(
                    self._collection_name,
                    vectors_config=self._build_vectors_config(size),
                    sparse_vectors_config=self._collection_config.sparse_vectors
                    or None,
                    hnsw_config=self._collection_config.hnsw_config,
                )
                for idx in self._collection_config.payload_indexes:
                    self.client.create_payload_index(
                        collection_name=self._collection_name,
                        field_name=idx.field_name,
                        field_schema=idx.schema,
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to initialise Qdrant collection '{self._collection_name}'"
                ) from e

        self.vectorstore = AsyncQdrantVectorStore(
            client=self.client,
            async_client=self.async_client,
            collection_name=self._collection_name,
            embedding=self.embeddings,
            retrieval_mode=self._retrieval_mode,
            sparse_embedding=self._sparse_embedding,
            late_interaction_embedding=self._late_interaction_embedding,
            vector_name=_DENSE_VECTOR_NAME,
            validate_collection_config=False,
        )

        return self.vectorstore

    def _build_vectors_config(self, dense_size: int) -> dict[str, models.VectorParams]:
        """Return vectors_config for collection creation.

        Always returns a named dict so every collection uses the ``"dense"``
        vector name consistently across modes.
        """
        return {
            _DENSE_VECTOR_NAME: models.VectorParams(
                size=dense_size,
                distance=self._collection_config.distance,
            ),
            **self._collection_config.multi_vectors,
        }

    async def aclose(self):
        self.client.close()
        await self.async_client.close()
