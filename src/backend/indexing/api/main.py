from contextlib import asynccontextmanager
from typing import List, AsyncIterator
from logging import getLogger, INFO


from fastapi import FastAPI, UploadFile, File
from langchain_core.documents import Document
from langchain_huggingface.embeddings import HuggingFaceEndpointEmbeddings
from qdrant_client import AsyncQdrantClient, models

import src.backend.indexing.api.service as service
from src.backend.indexing.api.config import settings



logger = getLogger(__name__)
logger.setLevel(INFO)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Starting FastAPI setup...")

    logger.info("Connecting to TEI embedding server...")
    embeddings = HuggingFaceEndpointEmbeddings(model=settings.TEI_HOST_URL)
    # dummy call to check for health
    await embeddings.aembed_query("dummy")

    logger.info("Connecting to Qdrant server...")
    client = AsyncQdrantClient(
        url=settings.QDRANT_HOST_URL,
        port=settings.QDRANT_HOST_PORT,
    )

    logger.info(f"Checking if collection '{settings.QDRANT_COLLECTION_NAME}' exists...")
    if not await client.collection_exists(settings.QDRANT_COLLECTION_NAME):
        try:
            # Create the collection for the project
            # 
            # The system collection will have two indices - one for chats and one
            # for global projects.
            # In order to avoid global indexing,
            # we will use the default payload index.
            logger.info(f"Creating a new collection '{settings.QDRANT_COLLECTION_NAME}'...")
            await client.create_collection(
                settings.QDRANT_COLLECTION_NAME,
                vectors_config=models.VectorParams(
                    size=len(await embeddings.aembed_query("dummy")),
                    distance=models.Distance.COSINE,
                ),
                hnsw_config=models.HnswConfigDiff(
                    payload_m=16,
                    m=0, # should disable global indexing
                )
            )
        
            logger.info(f"Creating a new payload index for chats...")
            # create an multitenancy index for chats, separated by "metadata.chat_id"
            await client.create_payload_index(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                field_name="metadata.chat_id",
                field_schema=models.UuidIndexParams(
                    type=models.UuidIndexType.UUID,
                    is_tenant=True,
                ),
            )

            # create another multitenancy index for project IDs, 
            # separated by "metadata.project_id"
            logger.info(f"Creating a new payload index for projects...")
            await client.create_payload_index(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                field_name="metadata.project_id",
                field_schema=models.KeywordIndexParams(
                    type=models.KeywordIndexType.KEYWORD,
                    is_tenant=True,
                )
            )
        except Exception as e:
            logger.error(f"Error occured: {str(e)}")
            raise
    yield

    logger.info("Disconnecting Qdrant client...")
    await client.close()

    logger.info("Tearing down application...")


app = FastAPI(
    title="Basic File Ingestion with Celery",
    lifespan=lifespan,
)


@app.post(
    "/ingest",
    response_model=List[Document],
)
async def ingest_endpoint(file: UploadFile = File(...)) -> List[Document]:
    return await service.ingest(file)
