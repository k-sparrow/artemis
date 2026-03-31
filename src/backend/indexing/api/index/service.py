from typing import List, Optional
from uuid import UUID

from src.lib.core.ingestion import BasePipeline
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.types import ParsedChunk, UpsertResult


async def a_delete(
    namespace: UUID,
    pipeline: BasePipeline,
    obj_id: Optional[str] = None,
) -> None:
    """Delete documents from the vectorstore for *namespace*.

    If *obj_id* is provided, only chunks for that object are removed —
    both from the vectorstore and the record manager — via
    :meth:`pipeline.adelete_source`.

    If *obj_id* is ``None`` (namespace-level deletion), the pipeline is
    called with an empty document list.  Because the upserters use
    ``cleanup="scoped_full"`` and ``scoped_full_cleanup_source_ids`` will be
    an empty set, LangChain's ``aindex`` falls back to unscoped full cleanup —
    deleting every key tracked in the namespace's record manager and the
    corresponding vectorstore entries.
    """
    if obj_id is not None:
        await pipeline.adelete_source(obj_id)
    else:
        await pipeline.aprocess([])


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
            metadata={
                "source": chunk.source,
                "type": chunk.type,
                "obj_id": str(chunk.obj_id),
            },
        )
        for chunk in chunks
    ]

    normalizer = MetadataFieldNormalizer(fields={"namespace": str(namespace)})
    docs = await normalizer.anormalize(docs)

    return await pipeline.aprocess(docs)
