from typing import List
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import UploadFile
from langchain_core.documents import Document

from src.backend.celery import ingest_task


async def ingest(file: UploadFile) -> List[Document]:
    result = ingest_task.delay(await file.read(), file.filename)
    resulsts = result.get()
    return [
        Document(**doc_vals["kwargs"])  # actual content of Document is in "kwargs"
        for doc_vals in resulsts
    ]
