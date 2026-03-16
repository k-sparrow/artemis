from typing import List
from uuid import UUID

from src.lib.core.ingestion import BasePipeline
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.types import ParsedChunk, UpsertResult


async def a_index_and_ingest(
    chunks: List[ParsedChunk],
    pipeline: BasePipeline,
    namespace: UUID,
) -> UpsertResult:
    """Index pre-parsed chunks into the vectorstore.

    1. Convert each ParsedChunk to a LangChain Document
    2. Stamp namespace metadata onto every document
    3. Run the pipeline (embed + upsert to Qdrant / record manager)
    """
    from langchain_core.documents import Document

    docs = [
        Document(
            page_content=chunk.page_content,
            metadata={"source": chunk.source, "type": chunk.type},
        )
        for chunk in chunks
    ]

    normalizer = MetadataFieldNormalizer(fields={"namespace": str(namespace)})
    docs = await normalizer.anormalize(docs)

    return await pipeline.aprocess(docs)
