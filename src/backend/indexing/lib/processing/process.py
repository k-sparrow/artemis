# core
from typing import List, Union, Dict
import uuid

# third party
from fastapi import UploadFile
from langchain_core.documents import Document
from langchain_core.runnables import RunnableLambda
from langchain.chains.summarize.chain import load_summarize_chain
from langchain_core.vectorstores import VectorStore
from sqlalchemy.ext.asyncio import AsyncEngine

# our code
from src.backend.indexing.lib.exceptions import DocumentProcessingException
from src.backend.indexing.lib.processing.pipeline import (
    Pipeline,
    SimpleRAGIndexingPipeline,
)
# TODO: get_lc_loader needs to be implemented or imported from correct location
# from src.backend.indexing.lib.utils import get_lc_loader


_all_ = [
    "aingest_and_upsert_documents",
    "ingest_and_upsert_documents",
]


# TODO: change the metadata assembly to include namespace in case of a library upload
async def aingest_and_upsert_documents(
    *,
    file: Union[UploadFile, None],
    doc_metadata: Dict[str, str],
    namespace: str,
    engine: AsyncEngine,
    vector_store: VectorStore,
) -> dict:
    """
    Ingest documents uploaded by users, breaks down to chunks, embeds and uploads
    to the vectorstore.

    Chunk entries updates are managed by an SQL record manager, provided by Langchain
    """

    # docs is empty in case we are deleting a document from the vectorstore
    try:
        docs: List[Document] = []

        # if we're ingesting and not deleting:
        if file:
            # force to read file from the beginning
            await file.seek(0)

            # load the file contents
            # TODO: this should be a part of indexing pipleine
            loader = get_lc_loader(
                file=await file.read(),
                filename=file.filename,  # type: ignore
                content_type=file.content_type,  # type: ignore
            )

            async for doc in loader.alazy_load():
                # add "chat_id" and "user_id" to the metadata object
                doc.metadata.update(
                    {
                        "source": file.filename,
                        **doc_metadata,
                    }
                )
                docs.append(doc)

        # create an ingestion pipeline - decide here what ingestion strategy
        # we want to use, and then upsert results to the vectorstore
        pipeline: Pipeline = SimpleRAGIndexingPipeline(
            namespace=namespace,
            engine=engine,
            vectorstore=vector_store,
        )
        await pipeline.aprocess(documents=docs)

        return {
            "source": file.filename if file else None,
            "namespace": namespace,
            "metadata": doc_metadata,
        }
    except Exception as e:
        raise DocumentProcessingException(message=str(e))


def ingest_and_upsert_documents(
    *,
    file: Union[UploadFile, None],
    namespace: str,
    doc_metadata: Dict[str, str],
    engine: AsyncEngine,
    vector_store: VectorStore,
) -> dict:
    """
    Sync version of "aingest_and_upsert_documents". For debug purposes only.
    Production strictly uses aingest_and_upsert_documents.

    Ingest documents uploaded by users, breaks down to chunks, embeds and uploads
    to the vectorstore.

    Chunk entries updates are managed by an SQL record manager, provided by Langchain
    """
    # docs is empty in case we are deleting a document from the vectorstore
    docs: List[Document] = []

    try:
        # if we're ingesting and not deleting:
        if file:
            # force to read file from the beginning
            file.file.seek(0)

            # load the file contents
            # TODO: this should be a part of indexing pipleine
            loader = get_lc_loader(
                file=file.file.read(),  # type: ignore
                filename=file.filename,  # type: ignore
                content_type=file.content_type,  # type: ignore
            )

            for doc in loader.lazy_load():
                # add "chat_id" and "user_id" to the metadata object
                doc.metadata.update(
                    {
                        "source": file.filename,
                        **doc_metadata,
                    }
                )
                docs.append(doc)

        # create an ingestion pipeline - decide here what ingestion strategy
        # we want to use, and then upsert results to the vectorstore
        pipeline: Pipeline = SimpleRAGIndexingPipeline(
            namespace=namespace,
            engine=engine,
            vectorstore=vector_store,
        )
        pipeline.process(documents=docs)

        return {
            "source": file.filename if file else None,
            "namespace": namespace,
            "metadata": doc_metadata,
        }
    except Exception as e:
        raise DocumentProcessingException(message=str(e))
