from typing import List

from fastapi import FastAPI, UploadFile, File
from langchain_core.documents import Document

import src.backend.indexing.api.service as service

app = FastAPI(
    title="Basic File Ingestion with Celery",
)


@app.post(
    "/ingest",
    response_model=List[Document],
)
async def ingest_endpoint(file: UploadFile = File(...)) -> List[Document]:
    return await service.ingest(file)
