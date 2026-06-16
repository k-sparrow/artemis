import uuid
from typing import Any, List, Optional, Protocol, Tuple, runtime_checkable

import httpx
from langchain_core.documents import Document

from src.lib.core.adapters.loaders import LoaderError, LoaderFactory
from src.lib.core.ingestion.exceptions import (
    DocumentProcessingException,
    UpstreamServiceException,
)
from src.lib.core.ingestion.normalizer.metadata import MetadataFieldNormalizer
from src.lib.core.ingestion.types import ChunkType, Page, ParseArtifact, ParsedChunk

__all__ = ["a_parse", "encode_artifact", "artifact_key", "replay_key"]


@runtime_checkable
class _ArtifactLoader(Protocol):
    """Loader capable of producing a page-aware parse artifact.

    Only the Docling loader implements this; the parse service narrows to it at
    runtime so a misconfigured (non-Docling) loader fails loudly rather than
    silently dropping pages. ``dl_doc`` is intentionally typed ``Any`` — the
    service treats it opaquely (only ``model_dump_json`` for the replay cache).
    """

    async def aload_artifact(
        self,
    ) -> Tuple[List[Document], List[Tuple[int, str]], Any]: ...


async def a_parse(
    content: bytes,
    filename: Optional[str],
    content_type: Optional[str],
    loader_factory: LoaderFactory,
    metadata: dict[str, str],
) -> Tuple[ParseArtifact, bytes]:
    """Parse raw *content* into a :class:`ParseArtifact` plus replay-cache bytes.

    Input bytes are resolved by the router from either an inline upload or a
    claim-check ``BlobRef``. Returns ``(artifact, replay)`` where ``artifact``
    carries page parents + agnostic chunks and ``replay`` is the lossless
    ``DoclingDocument`` JSON the router persists to the private replay bucket.
    """
    loader = loader_factory(content, filename, content_type)
    if not isinstance(loader, _ArtifactLoader):
        raise DocumentProcessingException(
            "configured loader does not support page-aware artifacts"
        )

    try:
        chunk_docs, page_tuples, dl_doc = await loader.aload_artifact()
    except LoaderError as exc:
        raise DocumentProcessingException(str(exc)) from exc
    except httpx.HTTPError as exc:
        raise UpstreamServiceException(
            service="document-loader",
            message=f"Failed to parse object '{filename}': {exc}",
        ) from exc

    if metadata:
        normalizer = MetadataFieldNormalizer(fields=metadata)
        chunk_docs = normalizer.normalize(chunk_docs)

    artifact = ParseArtifact(
        pages=[Page(page_no=page_no, markdown=md) for page_no, md in page_tuples],
        chunks=[_to_parsed_chunk(doc) for doc in chunk_docs],
    )
    replay = dl_doc.model_dump_json().encode()
    return artifact, replay


def encode_artifact(artifact: ParseArtifact) -> bytes:
    """Serialise the parse artifact to JSON bytes for object storage."""
    return artifact.model_dump_json().encode()


def artifact_key(obj_id: str) -> str:
    """Object-storage key for an object's parse artifact (idempotent per obj_id)."""
    return f"parse/{obj_id}.json"


def replay_key(obj_id: str) -> str:
    """Private replay-cache key for an object's lossless ``DoclingDocument``."""
    return f"replay/{obj_id}.json"


def _to_parsed_chunk(doc: Document) -> ParsedChunk:
    metadata = doc.metadata
    raw_type = metadata.get("type", ChunkType.TEXT)
    try:
        chunk_type = ChunkType(raw_type)
    except ValueError:
        chunk_type = ChunkType.UNKNOWN
    return ParsedChunk(
        page_content=doc.page_content,
        source=metadata.get("source", ""),
        type=chunk_type,
        obj_id=uuid.UUID(metadata["obj_id"]),
        page_no=metadata.get("page_no"),
    )
