import json
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
    "resolved_marker_key",
    "finalized_marker_key",
    "origin_filename_from_head",
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


def resolved_marker_key(obj_id: str) -> str:
    """Key of the marker recording which conversion task /v1/parse/resolve
    last fully cached for this object (its body is that parsing_task_id).

    Makes resolve safe to retry: a repeat call for the same task returns
    immediately instead of re-fetching a result that's already been consumed
    (the scratch object is deleted, docling-serve may have evicted it). Keyed
    on the task id rather than mere presence of the cache so a re-ingestion
    of the same obj_id never short-circuits onto the previous version.
    """
    return f"resolved/{obj_id}"


def finalized_marker_key(obj_id: str) -> str:
    """Key of the marker recording which chunk task /v1/chunk/finalize last
    wrote the parse artifact from (its body is that chunking_task_id). Same
    retry-safety role as :func:`resolved_marker_key`.
    """
    return f"finalized/{obj_id}"


def encode_pages(pages: List[Page]) -> bytes:
    """Serialise page parents to JSON bytes for the private pages cache."""
    return _pages_adapter.dump_json(pages)


def decode_pages(data: bytes) -> List[Page]:
    """Deserialise page parents from the private pages cache."""
    return _pages_adapter.validate_json(data)


def origin_filename_from_head(head: bytes) -> str:
    """``origin.filename`` of a serialised ``DoclingDocument``, read from just
    the first bytes of its JSON.

    ``origin`` is one of the first fields docling writes (before the body and
    text items that make up nearly all of the file), so the leading chunk of
    the object is enough — the point being to label pages with the same
    ``source`` as :func:`build_pages` without downloading and validating a
    document that can be hundreds of MB. Returns "" when ``origin`` is absent,
    null, or not found in *head*, same as ``build_pages`` does for a document
    with no origin.
    """
    text = head.decode("utf-8", errors="ignore")
    marker = '"origin":'
    start = text.find(marker)
    if start == -1:
        return ""
    start += len(marker)
    while start < len(text) and text[start].isspace():
        start += 1
    try:
        origin, _ = json.JSONDecoder().raw_decode(text, start)
    except ValueError:
        return ""
    if not isinstance(origin, dict):
        return ""
    return origin.get("filename") or ""
