# core
import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Iterator, AsyncIterator, Optional, Tuple, cast

# third party
from docling.chunking import BaseChunk, BaseChunker, HybridChunker
from docling.datamodel.document import DoclingDocument
from docling_core.types.doc.labels import DocItemLabel
from docling_core.transforms.chunker.doc_chunk import DocMeta
import httpx
from langchain_core.document_loaders import BaseLoader
from langchain_core.documents import Document

# our code
from src.lib.core.adapters.loaders.exceptions import LoaderError

__all__ = [
    "DoclingServeAPIDocumentConverter",
    "DoclingAPIServeLoader",
    "DoclingConversionError",
]


class DoclingConversionError(LoaderError):
    """Raised when Docling returns 200 but reports conversion errors in the response body.

    Attributes:
        errors: The raw ``errors`` list from the Docling response, each entry
            being a dict with ``component_type``, ``module_name``, and ``error_message``.
    """

    def __init__(self, errors: list[dict]) -> None:
        self.errors = errors
        messages = "; ".join(e.get("error_message", str(e)) for e in errors)
        super().__init__(f"Docling conversion failed: {messages}")


# TODO:
# Create a custom DoclingLoader which DOES NOT use docling.DocumentConverter
# as it tries to load rtree which causes a problem with libspatialindex_c

DEFAULT_DOCLING_SERVE_BASE_URI = os.environ.get(
    "DOCLING_SERVE_BASE_URI", "http://localhost:5001"
)
DEFAULT_CONVERTER_KWARGS = {
    "from_formats": ["docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "xlsx"],
    "to_formats": ["json", "text", "doctags"],
    "image_export_mode": "placeholder",
    "do_ocr": True,
    "force_ocr": False,
    "ocr_engine": "easyocr",
    "ocr_lang": ["en"],
    "pdf_backend": "dlparse_v4",
    "table_mode": "accurate",
    "do_table_structure": True,
    "abort_on_error": False,
}

DEFAULT_DOCLING_SOURCE_ENDPOINT = "/v1/convert/file"


class ExportType(str, Enum):
    """Enumeration of available export types."""

    MARKDOWN = "markdown"
    DOC_CHUNKS = "doc_chunks"


class BaseMetaExtractor(ABC):
    """BaseMetaExtractor."""

    @abstractmethod
    def extract_chunk_meta(self, file_path: str, chunk: BaseChunk) -> dict[str, Any]:
        """Extract chunk meta."""
        raise NotImplementedError()

    @abstractmethod
    def extract_dl_doc_meta(
        self, file_path: str, dl_doc: DoclingDocument
    ) -> dict[str, Any]:
        """Extract Docling document meta."""
        raise NotImplementedError()


class MetaExtractor(BaseMetaExtractor):
    """MetaExtractor."""

    def extract_chunk_meta(self, file_path: str, chunk: BaseChunk) -> dict[str, Any]:
        doc_meta = cast(DocMeta, chunk.meta)
        labels = [item.label for item in doc_meta.doc_items]
        # TABLE takes priority for mixed chunks; otherwise use the first item's label.
        if DocItemLabel.TABLE in labels:
            primary_label = DocItemLabel.TABLE
        elif labels:
            primary_label = labels[0]
        else:
            primary_label = DocItemLabel.TEXT
        return {
            "source": file_path,
            "type": primary_label.value,
            "dl_meta": chunk.meta.export_json_dict(),
        }

    def extract_dl_doc_meta(
        self, file_path: str, dl_doc: DoclingDocument
    ) -> dict[str, Any]:
        """Extract Docling document meta."""
        return {"source": file_path}


FileInput = Tuple[str, bytes, str]  # (filename, bytes, content_type)


class DoclingServeAPIDocumentConverter:
    def __init__(
        self, base_url: str, endpoint: str = DEFAULT_DOCLING_SOURCE_ENDPOINT, **kwargs
    ):
        self.client: httpx.Client = httpx.Client(base_url=base_url, timeout=12000000)
        self.async_client = httpx.AsyncClient(
            base_url=base_url,
            timeout=12000000,
        )
        self.__endpoint = endpoint
        self.__parameters = kwargs

    def convert(self, source: FileInput) -> DoclingDocument:
        # Just call the API's /v1/convert/source endpoint
        filename, content, content_type = source

        files = {"files": (filename, content, content_type)}
        # call the docling-serve API
        result = self.client.post(self.__endpoint, files=files, data=self.__parameters)
        result.raise_for_status()

        # convert the result back to DoclingDocument
        json_response = result.json()
        if errors := json_response.get("errors"):
            raise DoclingConversionError(errors)
        return DoclingDocument.model_validate(json_response["document"]["json_content"])

    async def aconvert(self, source: FileInput) -> DoclingDocument:
        # Just call the API's /v1/convert/source endpoint
        filename, content, content_type = source

        files = {"files": (filename, content, content_type)}
        # call the docling-serve API
        result = await self.async_client.post(
            self.__endpoint, files=files, data=self.__parameters
        )
        result.raise_for_status()

        # convert the result back to DoclingDocument
        json_response = result.json()
        if errors := json_response.get("errors"):
            raise DoclingConversionError(errors)
        return DoclingDocument.model_validate(json_response["document"]["json_content"])


class DoclingAPIServeLoader(BaseLoader):
    """Docling Loader."""

    def __init__(
        self,
        source: FileInput,
        *,
        converter: Optional[DoclingServeAPIDocumentConverter] = None,
        convert_kwargs: Optional[Dict[str, Any]] = None,
        chunker: Optional[BaseChunker] = None,
        meta_extractor: Optional[BaseMetaExtractor] = None,
        export_type: ExportType = ExportType.DOC_CHUNKS,
        md_export_kwargs: Optional[dict[str, Any]] = None,
    ):
        """Initialize with a file path.

        Args:
            file_path: File source as single str (URL or local file) or Iterable
                thereof.
            converter: Any specific `DocumentConverter` to use. Defaults to `None` (i.e.
                converter defined internally).
            convert_kwargs: Any specific kwargs to pass to conversion invocation.
                Defaults to `None` (i.e. behavior defined internally).
            chunker: Any specific `BaseChunker` to use (in case of
                `ExportType.DOC_CHUNKS`). Defaults to `None` (i.e. chunker defined
                internally).
            meta_extractor: The extractor instance to use for populating the output
                document metadata; if not set, a system default is used.
        """
        self._file = source

        self._convert_kwargs = (
            convert_kwargs if convert_kwargs is not None else DEFAULT_CONVERTER_KWARGS
        )
        self._converter: DoclingServeAPIDocumentConverter = (
            converter
            or DoclingServeAPIDocumentConverter(
                base_url=DEFAULT_DOCLING_SERVE_BASE_URI, **self._convert_kwargs
            )
        )
        self._chunker: BaseChunker = chunker or HybridChunker()
        self._meta_extractor = meta_extractor or MetaExtractor()
        self._export_type = export_type
        self._md_export_kwargs = (
            md_export_kwargs
            if md_export_kwargs is not None
            else {"image_placeholder": ""}
        )

    def lazy_load(
        self,
    ) -> Iterator[Document]:
        """Lazy load documents."""
        dl_doc: DoclingDocument = self._converter.convert(
            source=self._file,
        )
        chunk_iter = self._chunker.chunk(dl_doc)
        filename, _, _ = self._file
        if self._export_type == ExportType.MARKDOWN:
            yield Document(
                page_content=dl_doc.export_to_markdown(**self._md_export_kwargs),
                metadata=self._meta_extractor.extract_dl_doc_meta(
                    file_path=filename,
                    dl_doc=dl_doc,
                ),
            )
        elif self._export_type == ExportType.DOC_CHUNKS:
            chunk_iter = self._chunker.chunk(dl_doc)
            for chunk in chunk_iter:
                yield Document(
                    page_content=self._chunker.contextualize(chunk=chunk),
                    metadata=self._meta_extractor.extract_chunk_meta(
                        file_path=filename,
                        chunk=chunk,
                    ),
                )
        else:
            raise ValueError(f"Unexpected export type: {self._export_type}")

    async def alazy_load(self) -> AsyncIterator[Document]:
        dl_doc: DoclingDocument = await self._converter.aconvert(
            source=self._file,
        )
        chunk_iter = self._chunker.chunk(dl_doc)
        filename, _, _ = self._file
        if self._export_type == ExportType.MARKDOWN:
            yield Document(
                page_content=dl_doc.export_to_markdown(**self._md_export_kwargs),
                metadata=self._meta_extractor.extract_dl_doc_meta(
                    file_path=filename,
                    dl_doc=dl_doc,
                ),
            )
        elif self._export_type == ExportType.DOC_CHUNKS:
            chunk_iter = self._chunker.chunk(dl_doc)
            for chunk in chunk_iter:
                yield Document(
                    page_content=self._chunker.contextualize(chunk=chunk),
                    metadata=self._meta_extractor.extract_chunk_meta(
                        file_path=filename,
                        chunk=chunk,
                    ),
                )
        else:
            raise ValueError(f"Unexpected export type: {self._export_type}")
