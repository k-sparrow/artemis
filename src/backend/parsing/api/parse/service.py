from typing import List

from pydantic import TypeAdapter

from src.lib.core.ingestion.types import Page, ParseArtifact

__all__ = [
    "encode_artifact",
    "artifact_key",
    "replay_key",
    "pages_key",
    "encode_pages",
    "decode_pages",
    "convert_scratch_prefix",
]

_pages_adapter = TypeAdapter(List[Page])


def encode_artifact(artifact: ParseArtifact) -> bytes:
    """Serialise the parse artifact to JSON bytes for object storage."""
    return artifact.model_dump_json().encode()


def artifact_key(obj_id: str) -> str:
    """Object-storage key for an object's parse artifact (idempotent per obj_id)."""
    return f"parse/{obj_id}.json"


def replay_key(obj_id: str) -> str:
    """Private replay-cache key for an object's lossless ``DoclingDocument``."""
    return f"replay/{obj_id}.json"


def convert_scratch_prefix(obj_id: str) -> str:
    """Per-object scratch prefix docling-serve's S3Target writes the converted
    JSON under (submit_endpoint's source_ref path only). Read once by
    /v1/parse/resolve via a recursive listing, then deleted — never part of
    the parse->index contract.
    """
    return f"scratch/convert/{obj_id}/"


def pages_key(obj_id: str) -> str:
    """Private cache key for an object's page-level Markdown parents.

    Computed once by /v1/parse/resolve and consumed by /v1/chunk/finalize —
    chunking is a fully decoupled stage (Epic 21), so it never re-derives
    pages from the replay-cached DoclingDocument itself.
    """
    return f"pages/{obj_id}.json"


def encode_pages(pages: List[Page]) -> bytes:
    """Serialise page parents to JSON bytes for the private pages cache."""
    return _pages_adapter.dump_json(pages)


def decode_pages(data: bytes) -> List[Page]:
    """Deserialise page parents from the private pages cache."""
    return _pages_adapter.validate_json(data)
