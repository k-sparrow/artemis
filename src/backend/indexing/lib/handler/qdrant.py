from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from qdrant_client import AsyncQdrantClient, QdrantClient, models

from src.backend.indexing.lib.handler.base import BaseVectorHandler
from src.backend.indexing.lib.vectorstore import AsyncQdrantVectorStore

__all__ = [
    "QdrantVectorStoreHandler",
]


class QdrantVectorStoreHandler(BaseVectorHandler):
    client: AsyncQdrantClient | QdrantClient

    def __init__(
        self,
        embeddings: Embeddings,
        collection_name: str,
        base_url,
        eager: bool = False,
        **kwargs,
    ):
        self._collection_name = collection_name
        self._base_url = base_url
        self.client = None
        super().__init__(embeddings=embeddings, eager=eager, **kwargs)

    async def acreate(self) -> VectorStore:
        self.client = QdrantClient(url=self._base_url, prefer_grpc=False)

        if not self.client.collection_exists(self._collection_name):
            try:
                self.client.create_collection(
                    self._collection_name,
                    vectors_config=models.VectorParams(
                        # this is a stupid way to get the size of the embeddings
                        # but there's no other, unfortunately
                        size=len(self.embeddings.embed_query("dummy")),
                        distance=models.Distance.COSINE,
                    ),
                    hnsw_config=models.HnswConfigDiff(
                        payload_m=16,
                        m=0,  # should disable global indexing
                    ),
                )

                # create an multitenancy index for chats, separated by "metadata.chat_id"
                self.client.create_payload_index(
                    collection_name=self._collection_name,
                    field_name="metadata.chat_id",
                    field_schema=models.UuidIndexParams(
                        type=models.UuidIndexType.UUID,
                        is_tenant=True,
                    ),
                )

                # create another multitenancy index for project IDs,
                # separated by "metadata.project_id"
                self.client.create_payload_index(
                    collection_name=self._collection_name,
                    field_name="metadata.project_id",
                    field_schema=models.KeywordIndexParams(
                        type=models.KeywordIndexType.KEYWORD,
                        is_tenant=True,
                    ),
                )

                return AsyncQdrantVectorStore(
                    client=self.client,
                    collection_name=self._collection_name,
                    embedding=self.embeddings,
                )
            except Exception as e:
                raise

    async def aclose(self):
        await self.client.close()
