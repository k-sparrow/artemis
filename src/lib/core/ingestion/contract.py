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
    "IngestionInfo",
    "IngestionTaskDetails",
]


class S3Details(BaseModel):
    """Location of the object in MinIO / S3."""

    bucket: str
    object: str


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


class IngestionInfo(BaseModel):
    """Namespace context for the ingestion task."""

    namespace_id: UUID


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
