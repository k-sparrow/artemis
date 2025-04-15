from typing import Optional, List

from langchain_qdrant import QdrantVectorStore

__all__ = [
    "AsyncQdrantVectorStore",
]


# patch for QdrantVectorStore that implements adelete
class AsyncQdrantVectorStore(QdrantVectorStore):
    def _init_(
        self,
        **kwargs,
    ):
        super()._init_(**kwargs)

    # patch to make aindex work
    #
    # Langchain's aindex complains that QdrantVectorStore doesn't implement 'adelete'
    # so it can't be used because Langchain's base VectorStore::adelete throws an error
    # However, VectorStore::adelete is implemented, so aindex is out of sync with
    # the rest of Langchain's ecosystem.
    # Nevertheless, we need to monkey-patch it here to make it work with aindex
    async def adelete(self, ids: Optional[List[str]] = None, **kwargs):
        return await super().adelete(ids, **kwargs)
