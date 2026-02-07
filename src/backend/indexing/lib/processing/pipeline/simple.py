# third party
from langchain.indexes import SQLRecordManager
from langchain.vectorstores import VectorStore
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import Engine

# our code
from src.backend.indexing.lib.processing.ingest.simple import (
    SimpleRAGIngestor,
)
from src.backend.indexing.lib.processing.pipeline.base import Pipeline
from src.backend.indexing.lib.processing.upsert.simple import (
    SimpleRAGUpserter,
)

__all__ = [
    "SimpleRAGIndexingPipeline",
]


class SimpleRAGIndexingPipeline(Pipeline):
    """
    Simple RAG pipeline using recursive character splitting
    and simple upsertion to vectorstore via a single record manager that manages
    the chunks inside the vectorstore.

    The pipeline is per-document, a.k.a the list of Document objects as input for
    SimpleRAGIngestor are of the same document origin, denoted as "doc_id"
    """

    def __init__(
        self,
        namespace: str,
        engine: Engine | AsyncEngine,
        vectorstore: VectorStore,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
    ):
        super().__init__(
            ingestor=SimpleRAGIndexer(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
            ),
            upserter=SimpleRAGUpserter(
                vectorstore=vectorstore,
                record_manager=SQLRecordManager(
                    namespace=namespace,
                    engine=engine,
                ),
            ),
        )
