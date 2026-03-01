from typing import List

from fastapi import UploadFile

from src.backend.indexing.api.dependencies import LoaderFactory
from src.lib.core.ingestion import BasePipeline


async def ingest(
    file: UploadFile,
    loader_factory: LoaderFactory,
    pipeline: BasePipeline,
) -> List[str]:
    content = await file.read()
    loader = loader_factory(content, file.filename, file.content_type)
    docs = loader.load()
    return await pipeline.aprocess(docs)
