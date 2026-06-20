# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
"""Ingestion task contract shared between the storage service and the Celery worker.

This module defines the canonical data contract that travels through the pipeline:

    Storage service
        → MinIO object metadata (task_id + contract JSON)
        → Kafka S3 event
        → KSQLDB reshape
        → RabbitMQ (Camel sink connector)
        → Celery worker

``IngestionTaskDetails`` is the ``kwargs`` struct of the Celery task message.
The storage service constructs it, serialises it with ``model_dump_json()``, and
stores it as a single ``contract`` key in the MinIO PUT metadata.  KSQLDB
deserialises each leaf field via ``EXTRACTJSONFIELD`` and rebuilds the struct as
the Kafka message value consumed by the Camel RabbitMQ sink connector.

``task_id`` is stored separately in MinIO metadata (not inside the contract) so
the Kafka Connect ``HeaderFrom$Value`` SMT can promote it to the
``CamelHeader.id`` AMQP header that Celery v2 requires — the SMT only reaches
top-level fields of the Kafka message value.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

__all__ = [
    "S3Details",
    "SourceDetails",
    "BlobRef",
    "IngestionInfo",
    "IngestionTaskDetails",
    "ObjectScope",
    "ObjectProperties",
    "ObjectMetadata",
    "IndexingOutcome",
    "IngestionResult",
]


class S3Details(BaseModel):
    """Location of the object in MinIO / S3."""

    bucket: str
    object: str
    size: int


class BlobRef(BaseModel):
    """Self-describing claim-check reference to bytes in object storage.

    Used for both the input file (passed to parsing) and the parse artifact
    (returned by parsing, threaded to indexing): the payload never crosses the
    wire, only this ``{bucket, key}`` pair. Carrying the bucket makes the
    reference self-describing — the consumer reads exactly where the producer
    wrote, with no shared bucket-name config to keep in sync.
    """

    bucket: str
    key: str


class SourceDetails(BaseModel):
    """Metadata describing the ingested object.

    ``source`` is both the human-readable display label and the input to
    ``uuid5(namespace_id, source)`` that produces ``obj_id``.  It is set by
    the storage service at upload time and stamped on every parsed chunk so
    the vector store retains a readable provenance field.

    ``obj_id`` is the record-manager deduplication pivot.  Two uploads with
    the same ``(namespace_id, source)`` produce the same ``obj_id`` — the
    second upload overwrites the first.  Passing ``force_new=True`` at the
    API level generates a unique discriminated source (``"{name}/{uuid4}"``),
    yielding a distinct ``obj_id`` and keeping both copies.
    """

    source: str
    content_type: str
    obj_id: UUID
    object_type: str


class ObjectScope(BaseModel):
    """Tenancy context — which namespace and group owns this object."""

    namespace_id: UUID
    group_id: UUID | None = None


IngestionInfo = ObjectScope


class ObjectProperties(BaseModel):
    """Intrinsic file/blob properties."""

    object_type: str
    content_type: str
    size_bytes: int | None


class ObjectMetadata(BaseModel):
    """Full descriptor of an ingested object."""

    id: UUID
    source: str
    scope: ObjectScope
    properties: ObjectProperties


class IndexingOutcome(BaseModel):
    """Result produced by the vector indexing service."""

    num_added: int
    num_skipped: int
    ids: list[str]


class IngestionResult(BaseModel):
    """Result of a completed ``tasks.index`` or ``tasks.delete_document`` task.

    Serialised as JSON into ``apollo_celery_taskmeta.result``.
    The ksqlDB CSAS reads this to populate ``ingested_objects`` and
    ``ingestion_tasks`` via two JDBC sink connectors.

    ``task_id`` is the **contract** task_id — the id the storage service returned
    to the caller (= the ``tasks.ingest`` entry task's id), propagated down the
    chain. It is carried here so the ksqlDB ``ingestion_tasks`` fan-out can key the
    row by it, instead of by the per-subtask Celery id that owns this result row.
    Without it, ``GET /tasks/{task_id}`` for the caller's id finds nothing.
    """

    object: ObjectMetadata
    indexing: IndexingOutcome
    operation: str
    task_id: str | None = None


class IngestionTaskDetails(BaseModel):
    """The ``kwargs`` payload of the ``tasks.ingest`` Celery task.

    Constructed by the storage service and deserialised by the Celery worker.
    Serialise with ``model_dump_json()`` for MinIO metadata storage;
    the resulting JSON string is the ``contract`` metadata key.
    """

    upload_action: str
    s3: S3Details
    source: SourceDetails
    info: IngestionInfo
