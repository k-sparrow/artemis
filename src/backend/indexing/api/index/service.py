from typing import List
import json

from fastapi import UploadFile
from langchain_core.documents import Document


from src.backend.indexing.api.config import settings
from src.backend.indexing.client.generated.client import Client
from src.backend.indexing.client.generated.types import Response, File
from src.backend.indexing.client.generated.api.default import (
    parse_document_stream_parse_file_post,
    parse_document_url_parse_url_post,
)
from src.backend.indexing.client.generated.models import (
    BodyParseDocumentStreamParseFilePost,
    ParseUrlRequest,
    ParseResponse,
    ParseResponseData,
)


class DoclingClient:
    client: Client

    def __init__(self, base_url):
        self.client = Client(base_url=base_url)

    async def __call__(
        self, *, file: UploadFile | None = None, url: str | None = None
    ) -> Document:
        if file is not None and url is not None:
            raise ValueError("'file' and 'url' are mutually exclusive")

        if file:
            return await self._parse_file(file=file)
        else:
            return await self._parse_url(url=url)

    async def _parse_file(self, file: UploadFile) -> Document:
        body = BodyParseDocumentStreamParseFilePost(
            file=File(
                payload=file.file,
                file_name=file.filename,
                mime_type=file.content_type,
            ),
            data=json.dumps({"include_json": False, "output_format": "markdown"}),
        )

        # send request to the docling API to parse the document
        response: Response[
            ParseResponse
        ] = await parse_document_stream_parse_file_post.asyncio_detailed(
            client=self.client,
            body=body,
        )

        data: ParseResponseData = response.parsed.data
        return Document(page_content=data.output)

    async def _parse_url(self, url: str) -> Document:
        body = ParseUrlRequest(
            url=url,
            include_json=False,
            output_format="markdown",
        )
        # send request to the docling API to parse the url
        reponse: Response[
            ParseResponse
        ] = await parse_document_url_parse_url_post.asyncio_detailed(
            client=Client,
            body=body,
        )

        data: ParseResponseData = reponse.parsed.data
        return Document(page_content=data.output)


async def ingest(file_or_url: UploadFile | str) -> List[Document]:
    # client = DoclingClient(base_url=settings.DOCLING_BACKEND_URL)
    # doc = await client(file=file_or_url)
    return [Document(page_content="Hello")]
