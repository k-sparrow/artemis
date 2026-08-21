"""Parameterised Kafka Connect connector config templates.

Each template function returns a complete connector config dict ready to pass
to ``KafkaConnect.create_connector()``.

The filesystem source connector writes to a dedicated Kafka topic. The HTTP
sink connector (posting events to the enterprise ingest service) is a separate,
statically-deployed connector — not managed by the control plane.
"""

from __future__ import annotations

from typing import Any, Sequence

_FILESYSTEM_TOPIC = "artemis.datasource.filesystem"

# Extensions an admin may choose to ingest. Anything not on this list is
# rejected at the API layer (schemas.DataSourceCreate) before a connector is
# ever rendered, so docling-serve never sees files it can't parse.
ALLOWED_FILE_EXTENSIONS: tuple[str, ...] = (
    "pdf",
    "docx",
    "pptx",
    "xlsx",
    "html",
    "htm",
    "md",
    "txt",
    "csv",
    "png",
    "jpg",
    "jpeg",
    "tiff",
    "bmp",
)

DEFAULT_FILE_EXTENSIONS: tuple[str, ...] = (
    "pdf",
    "docx",
    "pptx",
    "xlsx",
    "html",
    "htm",
    "md",
    "txt",
    "csv",
)


# Camel's file consumer re-lists the entire watched tree on every poll and
# relies solely on this cache to skip already-seen files (noop=true means
# there's no other marker). The default MemoryIdempotentRepository holds only
# 1000 entries with silent LRU eviction: once a tree exceeds that, evicted
# files look "new" again on the next poll, forever - verified experimentally
# to produce unbounded duplicate ingestion (100k+ duplicates within minutes)
# with the task never failing or even logging a warning. Sized well above any
# expected tree; cheap to raise further since entries are just path strings.
_IDEMPOTENT_CACHE_SIZE = 50_000

# Default poll delay (500ms) is tuned for low-latency queues, not a RO/noop
# directory scan. A slower cycle both reduces filesystem load and shrinks the
# blast radius of the eviction issue above by the same factor, should the
# cache size ever be undersized again.
_POLL_DELAY_MS = 600_000


def render_filesystem_connector(
    *,
    connector_name: str,
    connector_id: str,
    watch_path: str,
    namespace: str,
    namespace_id: str,
    org_name: str,
    owner_id: str,
    recursive: bool = True,
    file_extensions: Sequence[str] = DEFAULT_FILE_EXTENSIONS,
    idempotent_cache_size: int = _IDEMPOTENT_CACHE_SIZE,
    poll_delay_ms: int = _POLL_DELAY_MS,
) -> dict[str, Any]:
    """Render a Camel FileSource connector config.

    Produces messages on ``artemis.datasource.filesystem``:
      - key:     ``<namespace>`` (plain string)
      - value:   raw file content (string)
      - headers: ``artemis.namespace``, ``artemis.org_name``,
                 ``CamelHeader.CamelFileAbsolutePath``

    Value reshaping (wrapping content, injecting structured metadata) must be
    done at the sink level — SMTs run before the converter materialises the
    record value, so ``HoistField$Value`` would receive a raw ``GenericFile``
    Java object rather than a String.

    Transform pipeline
    ------------------
    Key transforms:
      1. HoistKeyField            file-path string → ``{"path": <path>}``
      2. InsertNamespaceKey       → ``{"path": <path>, "namespace": <ns>}``
      3. RemovePathKey            → ``{"namespace": <ns>}``
      4. ExtractNamespaceKeyField → ``<ns>`` (plain string)
      5. DropCamelHeaders         drop redundant Camel.* headers

    Header injection:
      6. InjectNamespaceHeader    → header ``artemis.namespace = <ns>``
      7. InjectOrgNameHeader      → header ``artemis.org_name = <org>``

    The filesystem is assumed to be mounted read-only (PVC in RO mode), so
    ``noop=true`` is mandatory — the connector must never move or delete files.

    Args:
        connector_name: Kafka Connect connector name (``artemis-{id}``).
        watch_path: Absolute path to the directory to watch (container path).
        namespace: Artemis namespace display name.
        namespace_id: Artemis namespace UUID (from storage service POST /namespaces).
        org_name: Organisation name.
        recursive: Whether to watch subdirectories recursively.
        file_extensions: File extensions (no leading dot) to ingest. Rendered
            as ``camel.source.endpoint.includeExt`` so Camel drops everything
            else (e.g. video files) before it ever reaches Kafka.
        idempotent_cache_size: Capacity of the in-memory idempotent repository
            that de-duplicates already-seen files across polls. Must comfortably
            exceed the watched tree's file count or files will be silently
            evicted and reprocessed (see ``_IDEMPOTENT_CACHE_SIZE``).
        poll_delay_ms: Milliseconds between full tree scans.

    Returns:
        Connector config dict for ``KafkaConnect.create_connector()``.
    """
    config: dict[str, Any] = {
        "connector.class": (
            "org.apache.camel.kafkaconnector.file.CamelFileSourceConnector"
        ),
        "tasks.max": "1",
        "camel.source.path.directoryName": watch_path,
        "camel.source.endpoint.recursive": str(recursive).lower(),
        # noop=true: filesystem is RO-mounted; never move or delete files.
        "camel.source.endpoint.noop": "true",
        "camel.source.endpoint.idempotent": "true",
        "camel.source.endpoint.idempotentRepository": (
            "#class:org.apache.camel.support.processor.idempotent."
            "MemoryIdempotentRepository#memoryIdempotentRepository"
            f"({idempotent_cache_size})"
        ),
        "camel.source.endpoint.delay": str(poll_delay_ms),
        # Use the consumed file path as the raw key material.
        "camel.source.camelMessageHeaderKey": "CamelFileNameConsumed",
        "topics": _FILESYSTEM_TOPIC,
        # Key: plain string (namespace); Value: raw file content string.
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter.schemas.enable": "false",
        "transforms": ",".join(
            [
                # Key transforms
                "HoistKeyField",
                "InsertNamespaceKey",
                "RemovePathKey",
                "ExtractNamespaceKeyField",
                "DropCamelHeaders",
                # Header-only metadata injection
                "InjectNamespaceHeader",
                "InjectNamespaceIdHeader",
                "InjectOrgNameHeader",
                "InjectGroupIdHeader",
                "InjectOwnerIdHeader",
            ]
        ),
        # ── Key transforms ───────────────────────────────────────────────
        # 1. file-path string → {"path": <path>}
        "transforms.HoistKeyField.type": (
            "org.apache.kafka.connect.transforms.HoistField$Key"
        ),
        "transforms.HoistKeyField.field": "path",
        # 2. {"path": <path>} → {"path": <path>, "namespace": <ns>}
        "transforms.InsertNamespaceKey.type": (
            "org.apache.kafka.connect.transforms.InsertField$Key"
        ),
        "transforms.InsertNamespaceKey.static.field": "namespace",
        "transforms.InsertNamespaceKey.static.value": namespace,
        # 3. {"path": <path>, "namespace": <ns>} → {"namespace": <ns>}
        "transforms.RemovePathKey.type": (
            "org.apache.kafka.connect.transforms.ReplaceField$Key"
        ),
        "transforms.RemovePathKey.exclude": "path",
        # 4. {"namespace": <ns>} → <ns> (plain string)
        "transforms.ExtractNamespaceKeyField.type": (
            "org.apache.kafka.connect.transforms.ExtractField$Key"
        ),
        "transforms.ExtractNamespaceKeyField.field": "namespace",
        # 5. Drop redundant Camel.* headers
        "transforms.DropCamelHeaders.type": (
            "org.apache.kafka.connect.transforms.DropHeaders"
        ),
        "transforms.DropCamelHeaders.headers": ",".join(
            [
                "CamelHeader.CamelFileNameConsumed",
                "CamelHeader.CamelFileNameOnly",
                "CamelHeader.CamelFileRelativePath",
                "CamelHeader.CamelFileLength",
                "CamelHeader.CamelFileParent",
                "CamelHeader.CamelFileAbsolute",
            ]
        ),
        # ── Header injection ─────────────────────────────────────────────
        "transforms.InjectNamespaceHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.InjectNamespaceHeader.header": "artemis.namespace",
        "transforms.InjectNamespaceHeader.value.literal": namespace,
        "transforms.InjectNamespaceIdHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.InjectNamespaceIdHeader.header": "artemis.namespace_id",
        "transforms.InjectNamespaceIdHeader.value.literal": namespace_id,
        "transforms.InjectOrgNameHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.InjectOrgNameHeader.header": "artemis.org_name",
        "transforms.InjectOrgNameHeader.value.literal": org_name,
        "transforms.InjectGroupIdHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.InjectGroupIdHeader.header": "artemis.group_id",
        "transforms.InjectGroupIdHeader.value.literal": connector_id,
        "transforms.InjectOwnerIdHeader.type": (
            "org.apache.kafka.connect.transforms.InsertHeader"
        ),
        "transforms.InjectOwnerIdHeader.header": "artemis.owner_id",
        "transforms.InjectOwnerIdHeader.value.literal": owner_id,
    }
    if file_extensions:
        config["camel.source.endpoint.includeExt"] = ",".join(file_extensions)
    return {"name": connector_name, "config": config}
