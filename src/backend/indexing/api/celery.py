import os
import time
from pathlib import Path
from typing import List
from tempfile import NamedTemporaryFile

from celery import Celery, shared_task
from fastapi import UploadFile
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_core.documents import Document

CELERY_BROKER_URL = os.environ.get(
    "CELERY_BROKER_URL", default="redis://localhost:6379/0"
)
CELERY_RESULT_BACKEND = os.environ.get(
    "CELERY_RESULT_BACKEND", default="redis://localhost:6379/0"
)

celery = Celery(__name__)
celery.conf.broker_url = CELERY_BROKER_URL
celery.conf.result_backend = CELERY_RESULT_BACKEND

__all__ = ["ingest_task"]


@shared_task
def ingest_task(io: bytes, filename: str) -> List[Document]:
    with NamedTemporaryFile(mode="w+b", suffix=Path(filename).suffix) as f:
        f.write(io)
        f.seek(0)
        loader = PyMuPDFLoader(f.name)
        docs: List[Document] = loader.load()
        for doc in docs:
            doc.metadata["source"] = filename

    return [doc.to_json() for doc in docs]
