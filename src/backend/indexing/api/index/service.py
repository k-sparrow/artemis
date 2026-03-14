from uuid import UUID

import httpx
from fastapi import UploadFile

from src.backend.indexing.api.dependencies import LoaderFactory
from src.lib.core.ingestion import BasePipeline
from src.lib.core.ingestion.exceptions import UpstreamServiceException
from src.lib.core.ingestion.normalizer import MetadataFieldNormalizer
from src.lib.core.ingestion.types import UpsertResult


async def a_index_and_ingest(
    file: UploadFile,
    loader_factory: LoaderFactory,
    pipeline: BasePipeline,
    namespace: UUID,
) -> UpsertResult:
    """
    Core functionality

    This function is responsible for:
    1. Reading the uploaded file
    2. Converting it to a list of Document objects (pymupdf4llm, docling)
    3. Stamping each Document's metadata (e.g., namespace)
    4. Upserting each Document to a vectorstore and DB
    5. Returning the list of chunk IDs upserted
    """
    content = await file.read()

    try:
        loader = loader_factory(content, file.filename, file.content_type)
        docs = loader.load()
    except httpx.HTTPError as exc:
        raise UpstreamServiceException(
            service="document-loader",
            message=f"Failed to parse document '{file.filename}': {exc}",
        ) from exc

    # stamp each Document with the namespace to which it belongs
    normalizer = MetadataFieldNormalizer(fields={"namespace": str(namespace)})
    docs = await normalizer.anormalize(docs)

    # process the chunks and upsert to storage
    return await pipeline.aprocess(docs)
