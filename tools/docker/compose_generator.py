#!/usr/bin/env python3
"""Generate docker-compose YAML for dev, test or release modes from a template.

Usage:
    compose_generator --mode dev|test|release --template PATH [--output PATH]

The template file uses {PLACEHOLDER} markers:

  {DOCLING_IMAGE}      Full image ref for docling-serve
  {DOCLING_GPU_BLOCK}  Multi-line YAML block for nvidia runtime + deploy section;
                       empty string in test mode (CPU-only Docling)
  {TEI_IMAGE}          Full image ref for HuggingFace TEI
  {TEI_MODEL}          Model ID passed to TEI at startup
  {TEI_GPU_BLOCK}      Multi-line YAML block for nvidia runtime + deploy section;
                       empty string in test mode (CPU-only TEI image)
  {COLBERT_IMAGE}          Full image ref for the ColBERT vLLM service (reused by its model-downloader)
  {COLBERT_MODEL}          Model ID for the ColBERT model-downloader init service
  {COLBERT_COMMAND_BLOCK}  Full YAML list-form command block for the ColBERT vLLM service
  {COLBERT_GPU_BLOCK}      Multi-line YAML block for nvidia runtime + deploy section
  {DOCLING_ENGINE_ENV_BLOCK}  Extra docling-serve `environment:` entries selecting the
                       Ray-backed engine (server-side PDF page-slice fan-out); set in
                       all three modes (Epic 21 §21.7)
  {DOCLING_OTEL_ENTRYPOINT_BLOCK}  docling-serve `entrypoint:` override computing the
                       OTel enable-flags via a real shell conditional; dev/release only,
                       empty string in test mode (no collector is ever configured there)
  {DOCLING_OTEL_ENV_BLOCK}  Companion `environment:` entries (OTEL_EXPORTER_OTLP_ENDPOINT,
                       DOCLING_SERVE_OTEL_SERVICE_NAME); dev/release only, empty in test
  {ARTEMIS_TAG_DEV}    Tag for artemis app images that ship a :dev variant
  {ARTEMIS_TAG_LATEST} Tag for artemis images that ship a :latest variant

Modes
-----
  dev      Local development. Artemis images at :dev/:latest; everything published.
  test     e2e via testcontainers. CPU TEI, no GPU mounts. Docling-serve runs the
           Ray-backed engine on the cu128 image but without a GPU reservation —
           the Ray cluster's actual conversion work falls back to CPU (slower, still
           correct); this deliberately reuses the cu128 image rather than switching to
           a lighter CPU-only tag since only the cu128 image is proven to bundle the
           Ray engine's dependencies (see TODOs.md Epic 21).
  release  Single-host staging/release of TAGGED images. Differs from dev by:
             - artemis images pinned to ${ARTEMIS_VERSION} (must be set)
             - dev-tools services dropped (the `# >>> dev-only` … `# <<< dev-only`
               block is removed)
             - gateway-only ingress: published host `ports:` stripped from every
               service except the apisix gateway (internal `expose:` kept)
           NOTE: still carries the dev secrets/passwords from the template — this
           is a staging surface, not hardened prod. Override secrets before real use.

  Conditional markers
  -------------------
  `# >>> dev-only` / `# <<< dev-only`   — removed in release mode only
  `# >>> deploy-only` / `# <<< deploy-only` — removed in test mode only
    Use deploy-only for model-downloader init services and their depends_on
    references: they are a deployment feature, not needed for e2e test containers.

Bazel integration
-----------------
  bazel run  //tools/docker:docker_compose.update    # writes deployment/docker/docker-compose.dev.yaml
  bazel build //tools/docker:docker_compose_test     # produces bazel-bin artefact
  bazel build //tools/docker:docker_compose_release  # produces bazel-bin artefact
"""

from __future__ import annotations

import argparse
import os
import sys

_GPU_DEPLOY_BLOCK = (
    "\n"
    "    deploy: # This section is for compatibility with Swarm\n"
    "      resources:\n"
    "        reservations:\n"
    "          devices:\n"
    "            - driver: nvidia\n"
    "              count: all\n"
    "              capabilities: [gpu]\n"
    "    runtime: nvidia"
)

# Pinned to ${ARTEMIS_VERSION}; the `:?` form makes the release compose refuse to
# start unless the operator pins a release tag (no accidental :latest/:dev).
_ARTEMIS_TAG_RELEASE = "${ARTEMIS_VERSION:?set ARTEMIS_VERSION to a release tag}"

# Markers in the template that bound the dev-tools section.
_DEV_ONLY_OPEN = "# >>> dev-only"
_DEV_ONLY_CLOSE = "# <<< dev-only"

# Markers for blocks that must be present in dev/release but stripped in test.
# Use these around model-downloader init services and their depends_on references —
# they are a deployment feature, not needed for e2e test containers.
_DEPLOY_ONLY_OPEN = "# >>> deploy-only"
_DEPLOY_ONLY_CLOSE = "# <<< deploy-only"

# docling-serve Ray engine — server-side PDF page-slice fan-out across a Ray
# cluster (ray-head + ray-worker, GCS backed by redis). Dev + test compose for
# now; release promotion deferred, see TODOs.md Epic 21 §21.7.
_DOCLING_RAY_ENGINE_ENV_BLOCK = (
    "\n"
    '      DOCLING_SERVE_ENG_KIND: "ray"\n'
    '      DOCLING_SERVE_ENG_RAY_ADDRESS: "ray://ray-head:10001"\n'
    '      DOCLING_SERVE_ENG_RAY_NAMESPACE: "docling"\n'
    '      DOCLING_SERVE_ENG_RAY_REDIS_URL: "redis://redis:6379/"\n'
    "      # Autoscaling pinned to 2 warm converter replicas, split across the 2\n"
    "      # ray-worker nodes started via `docker compose ... up --scale ray-worker=2`\n"
    "      # (see ray-worker's own comment). Both replicas share the box's single\n"
    "      # physical GPU via driver-level time-slicing (no MIG/MPS configured) —\n"
    "      # no VRAM partitioning, so this trades some VRAM headroom for overlap\n"
    "      # between one replica's CPU-bound phase and the other's GPU-bound phase.\n"
    "      # max_replicas_per_node=1 is required for the split, not just a hint —\n"
    "      # Ray's own default (None/uncapped) explicitly packs replicas for\n"
    "      # density, which would otherwise happily put both actors on one node\n"
    "      # (each fits in half its 4 CPUs) and leave the second node's CPU idle.\n"
    '      DOCLING_SERVE_ENG_RAY_MIN_ACTORS: "2"\n'
    '      DOCLING_SERVE_ENG_RAY_MAX_ACTORS: "2"\n'
    '      DOCLING_SERVE_ENG_RAY_CONVERTER_ACTOR_NUM_CPUS: "2"\n'
    '      DOCLING_SERVE_ENG_RAY_CONVERTER_MAX_REPLICAS_PER_NODE: "1"\n'
    "      # 2 slices in flight per replica x 2 replicas, matched at the per-tenant\n"
    "      # dispatch gate\n"
    '      DOCLING_SERVE_ENG_RAY_MAX_ONGOING_REQUESTS_PER_REPLICA: "2"\n'
    '      DOCLING_SERVE_ENG_RAY_MAX_CONCURRENT_TASKS: "4"\n'
    "      # PDF page-slice fan-out is off by default upstream — opt in explicitly.\n"
    "      # 150 pages/slice: below that a document converts as a single\n"
    "      # request (no fan-out overhead — converter-unit acquire/release,\n"
    "      # extra Ray remote round-trips, result reassembly); only genuinely\n"
    "      # huge documents split, into a slice count that actually matches\n"
    "      # the 2-replica/max_concurrent_tasks=4 capacity above instead of\n"
    "      # dozens of small slices queuing behind each other.\n"
    '      DOCLING_SERVE_ENG_RAY_ENABLE_PDF_PAGE_SLICE_FANOUT: "true"\n'
    '      DOCLING_SERVE_ENG_RAY_MAX_PAGE_SLICE_SIZE: "150"\n'
    "      # Same ~24h ceiling the pre-Ray local orchestrator relied on (client-side\n"
    "      # HTTPX_TIMEOUT/consumer_timeout, since superseded by Epic 18's async parse\n"
    "      # chain) — upstream's tight defaults (1h task / 5min document) truncate\n"
    "      # large PDFs converting under the Ray engine.\n"
    '      DOCLING_SERVE_ENG_RAY_TASK_TIMEOUT: "86400.0"\n'
    '      DOCLING_SERVE_ENG_RAY_DOCUMENT_TIMEOUT: "86400.0"\n'
    "      # Active backpressure: reject past 50 queued tasks/tenant, rather than\n"
    "      # queueing unboundedly. NOTE the var name is MAX_QUEUED_TASKS, not\n"
    "      # DEFAULT_MAX_QUEUED_TASKS — the latter is what .env.example's own main\n"
    '      # reference list (wrongly) documents; only its buried "Medium deployment"\n'
    "      # example has the name that actually matches the pydantic field. Confirmed\n"
    '      # live: the wrong name is silently swallowed (extra="ignore"), so the\n'
    "      # limit silently never enforces at all. Rejection itself currently\n"
    "      # returns a bare 500 on stock docling-serve — QueueLimitExceededError\n"
    "      # has no registered FastAPI exception handler (upstream issue #581,\n"
    "      # unmerged fix in PR #583) — patched to a real 429 in the\n"
    "      # docling-serve-ray fork image this compose file actually runs; the\n"
    "      # worker's submit_parse/submit_chunk (src/backend/controller/worker/tasks.py)\n"
    "      # treat 429 as expected backpressure — flat-delay retry, not the\n"
    "      # breaker/5xx failure path.\n"
    '      DOCLING_SERVE_ENG_RAY_MAX_QUEUED_TASKS: "50"\n'
    '      DOCLING_SERVE_ENG_RAY_ENABLE_QUEUE_LIMIT_REJECTION: "true"'
)

# docling-serve OTel gating. compose interpolates every scalar in the file —
# entrypoint included — before the container's shell ever sees it, and
# compose's own ${VAR:+true} has no "else" branch: when OTEL_EXPORTER_OTLP_ENDPOINT
# is unset that resolves to a literal empty string, which docling-serve's
# pydantic settings (env_parse_none_str="") maps to None, failing bool
# validation on its two *required* (non-Optional) otel_enable_* fields and
# crash-looping the container. A real shell conditional fixes it, but only if
# every `$` is escaped as `$$` so compose passes the ternary through literally
# instead of resolving it itself (statically, against the host env). Dev/release
# only — dropped in test mode, where no collector is ever configured and this
# whole conditional is dead weight (see TODOs.md Epic 21 §21.9 postscript).
_DOCLING_OTEL_ENTRYPOINT_BLOCK = (
    "\n"
    "    entrypoint:\n"
    "      - /bin/bash\n"
    "      - -c\n"
    '      - \'export DOCLING_SERVE_OTEL_ENABLE_TRACES="$${OTEL_EXPORTER_OTLP_ENDPOINT:+true}"; '
    'export DOCLING_SERVE_OTEL_ENABLE_TRACES="$${DOCLING_SERVE_OTEL_ENABLE_TRACES:-false}"; '
    'export DOCLING_SERVE_OTEL_ENABLE_OTLP_METRICS="$$DOCLING_SERVE_OTEL_ENABLE_TRACES"; '
    "exec docling-serve run'"
)
_DOCLING_OTEL_ENV_BLOCK = (
    "\n"
    '      OTEL_EXPORTER_OTLP_ENDPOINT: "${OTEL_EXPORTER_OTLP_ENDPOINT:-}"\n'
    '      DOCLING_SERVE_OTEL_SERVICE_NAME: "docling-serve"'
)

# Services whose published host ports survive in release mode (gateway ingress).
_RELEASE_PORT_ALLOWLIST = frozenset({"apisix"})

_RELEASE_BANNER = (
    "# ============================================================================\n"
    "# GENERATED — do not edit. Source: tools/docker/docker-compose.tmpl.yaml\n"
    "# Release/staging variant: artemis images pinned to ${ARTEMIS_VERSION},\n"
    "# dev-tools removed, gateway-only ingress.\n"
    "#\n"
    "#   ARTEMIS_VERSION=v1.0.0-alpha.2 docker compose \\\n"
    "#     -f deployment/docker/docker-compose.release.yaml --profile <p> up -d\n"
    "#\n"
    "# DOCLING_IMAGE is the one exception to ${ARTEMIS_VERSION}: it's pinned at\n"
    "# *build* time from //tools/oci/images/docling's own STABLE_VERSION stamp,\n"
    "# not deploy time. This checked-in file was generated unstamped, so it\n"
    "# shows the 0.0.0 placeholder below — regenerate with\n"
    "# `bazel build //tools/docker:docker_compose_release --stamp` for a real\n"
    "# release to get a traceable version instead.\n"
    "#\n"
    "# WARNING: still carries the template's DEV secrets/passwords — override them\n"
    "# (JWT_SECRET, MinIO/Postgres creds) before any non-staging use.\n"
    "# ============================================================================\n"
)

_DEV_SUBS: dict[str, str] = {
    # Pinned to the latest stable release (NOT a moving :main tag): the
    # parsing-service redesign depends on the /v1/chunk/hybrid response shape
    # (per-chunk page_numbers + documents[].md_content). cu128 GPU variant.
    # Bazel-built on top of that pinned base (see //tools/oci/images/docling) —
    # layers a one-line upstream docling-jobkit fix (SourceChunkConvertRequest.chunk
    # was DocumentChunk[Any, Any], an unpicklable subscripted generic that broke
    # every S3-source fan-out under Ray Serve; see docling/BUILD.bazel for the
    # full root cause). Build+load via `bazel run //:load.images.dev` before
    # `docker compose up` — same workflow as every other locally-built image.
    "DOCLING_IMAGE": "artemis/docling-serve-ray-patched:latest",
    "DOCLING_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
    "DOCLING_ENGINE_ENV_BLOCK": _DOCLING_RAY_ENGINE_ENV_BLOCK,
    "DOCLING_OTEL_ENTRYPOINT_BLOCK": _DOCLING_OTEL_ENTRYPOINT_BLOCK,
    "DOCLING_OTEL_ENV_BLOCK": _DOCLING_OTEL_ENV_BLOCK,
    "TEI_IMAGE": "ghcr.io/huggingface/text-embeddings-inference:1.6",
    "TEI_MODEL": "Alibaba-NLP/gte-large-en-v1.5",
    "TEI_POOLING": "mean",
    "TEI_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
    "COLBERT_IMAGE": "vllm/vllm-openai:v0.20.2",
    "COLBERT_MODEL": "jinaai/jina-colbert-v2",
    "COLBERT_COMMAND_BLOCK": (
        "\n"
        "    command:\n"
        "      - jinaai/jina-colbert-v2\n"
        "      - --pooler-config.task\n"
        "      - token_embed\n"
        "      - --hf-overrides\n"
        """      - '{"architectures": ["ColBERTJinaRobertaModel"]}'\n"""
        "      - --trust-remote-code\n"
        "      - --gpu-memory-utilization\n"
        "      - '0.10'"
    ),
    "COLBERT_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
    "ARTEMIS_TAG_DEV": "dev",
    "ARTEMIS_TAG_LATEST": "latest",
}

_TEST_SUBS: dict[str, str] = {
    # cu128, not the lighter :main CPU tag: the Ray engine's dependencies (Ray
    # runtime, redis client) are only proven present on cu128 — see TODOs.md
    # Epic 21. No GPU is reserved (DOCLING_GPU_BLOCK empty below), so this
    # still runs CPU-only, just from the same image dev/release use. Same
    # Bazel-built patched image as dev/release (see _DEV_SUBS) — dev/test/release
    # parity, and the Ray-serde patch matters here too if S3-source submission is
    # ever exercised in a test.
    "DOCLING_IMAGE": "artemis/docling-serve-ray-patched:latest",
    "DOCLING_GPU_BLOCK": "",
    "DOCLING_ENGINE_ENV_BLOCK": _DOCLING_RAY_ENGINE_ENV_BLOCK,
    # No collector is ever configured for e2e/testcontainers runs, so the
    # whole OTel-gating dance (see _DOCLING_OTEL_ENTRYPOINT_BLOCK) is dead
    # weight here — drop it and let the image's own default entrypoint run.
    "DOCLING_OTEL_ENTRYPOINT_BLOCK": "",
    "DOCLING_OTEL_ENV_BLOCK": "",
    "TEI_IMAGE": "ghcr.io/huggingface/text-embeddings-inference:cpu-1.6",
    "TEI_MODEL": "BAAI/bge-small-en-v1.5",
    "TEI_POOLING": "cls",
    "TEI_GPU_BLOCK": "",
    "COLBERT_IMAGE": "vllm/vllm-openai:v0.20.2",
    "COLBERT_MODEL": "colbert-ir/colbertv2.0",
    "COLBERT_COMMAND_BLOCK": (
        "\n"
        "    command:\n"
        "      - colbert-ir/colbertv2.0\n"
        "      - --pooler-config.task\n"
        "      - token_embed"
    ),
    "COLBERT_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
    "ARTEMIS_TAG_DEV": "dev",
    "ARTEMIS_TAG_LATEST": "latest",
}

# Release reuses the dev (GPU, prod-image, Ray-engine) substitutions unchanged —
# only the artemis tags differ, collapsing to the pinned ${ARTEMIS_VERSION}.
# Promoted to Ray 2026-08-07 (Epic 21 §21.7) ahead of a large-PDF (400+ page)
# memory-bounding proof — the correctness fan-out was verified at 50 pages/5
# slices, not at the scale the feature exists for; promoted anyway on an
# explicit, deliberate call to accept that risk rather than wait. If a
# large-PDF run later reveals a real memory-bounding problem, revert by
# re-adding "DOCLING_ENGINE_ENV_BLOCK": "" here.
_RELEASE_SUBS: dict[str, str] = {
    **_DEV_SUBS,
    "ARTEMIS_TAG_DEV": _ARTEMIS_TAG_RELEASE,
    "ARTEMIS_TAG_LATEST": _ARTEMIS_TAG_RELEASE,
}

_SUBS_BY_MODE = {"dev": _DEV_SUBS, "test": _TEST_SUBS, "release": _RELEASE_SUBS}


def _leading_spaces(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _strip_dev_only(lines: list[str], *, keep_content: bool) -> list[str]:
    """Drop the `# >>> dev-only` … `# <<< dev-only` markers.

    keep_content=True  → remove only the marker lines (dev/test: section stays).
    keep_content=False → remove the markers AND everything between them (release).
    """
    out: list[str] = []
    inside = False
    for line in lines:
        stripped = line.strip()
        if stripped == _DEV_ONLY_OPEN:
            inside = True
            continue
        if stripped == _DEV_ONLY_CLOSE:
            inside = False
            continue
        if inside and not keep_content:
            continue
        out.append(line)
    return out


def _strip_deploy_only(lines: list[str], *, keep_content: bool) -> list[str]:
    """Drop `# >>> deploy-only` … `# <<< deploy-only` marker blocks.

    keep_content=True  → remove only the marker lines (dev/release: section stays).
    keep_content=False → remove the markers AND everything between them (test).
    """
    out: list[str] = []
    inside = False
    for line in lines:
        stripped = line.strip()
        if stripped == _DEPLOY_ONLY_OPEN:
            inside = True
            continue
        if stripped == _DEPLOY_ONLY_CLOSE:
            inside = False
            continue
        if inside and not keep_content:
            continue
        out.append(line)
    return out


def _strip_unpublished_ports(lines: list[str]) -> list[str]:
    """Remove published `ports:` blocks for every service not in the allowlist.

    Tracks the current top-level block (anchor at col 0 or service at 2-space
    indent). A `ports:` key plus its deeper-indented list items is dropped unless
    the owning service is allow-listed (the gateway). `expose:` is untouched.
    """
    out: list[str] = []
    current_block: str | None = None
    skipping_ports = False
    ports_indent = 0

    for line in lines:
        indent = _leading_spaces(line)
        stripped = line.strip()

        if skipping_ports:
            # Consume list items / mapping under the ports: key.
            if stripped == "" or indent > ports_indent:
                continue
            skipping_ports = False  # fall through; re-classify this line

        # Track the owning block: anchors (`x-foo: &foo`, col 0) and services
        # (2-space indent, e.g. `  apisix:`).
        if indent == 0 and (stripped.endswith(":") or " &" in stripped):
            current_block = stripped.split(":", 1)[0]
        elif indent == 2 and stripped.endswith(":"):
            current_block = stripped[:-1]

        if stripped == "ports:" and current_block not in _RELEASE_PORT_ALLOWLIST:
            skipping_ports = True
            ports_indent = indent
            continue

        out.append(line)
    return out


def generate(template: str, mode: str, docling_image: str | None = None) -> str:
    subs = _SUBS_BY_MODE[mode]
    if docling_image:
        subs = {**subs, "DOCLING_IMAGE": docling_image}
    result = template
    for key, value in subs.items():
        result = result.replace("{" + key + "}", value)

    lines = result.split("\n")
    lines = _strip_dev_only(lines, keep_content=(mode != "release"))
    lines = _strip_deploy_only(lines, keep_content=(mode != "test"))
    if mode == "release":
        lines = _strip_unpublished_ports(lines)
    result = "\n".join(lines)

    if mode == "release":
        result = _RELEASE_BANNER + result
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["dev", "test", "release"],
        required=True,
        help="Target environment.",
    )
    parser.add_argument(
        "--template",
        required=True,
        help="Path to the docker-compose.tmpl.yaml template file.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Output path. When --mode=dev and omitted, writes to "
            "$BUILD_WORKSPACE_DIRECTORY/deployment/docker/docker-compose.dev.yaml."
        ),
    )
    parser.add_argument(
        "--docling-image",
        default=None,
        help=(
            "Override DOCLING_IMAGE (release mode only) — the "
            "STABLE_VERSION-stamped tag from "
            "//tools/oci/images/docling:tarball.tags, so release pins a real "
            "version instead of the mutable :latest dev tag. Falls back to "
            "the mode's own default when omitted."
        ),
    )
    args = parser.parse_args()

    with open(args.template, encoding="utf-8") as fh:
        template = fh.read()

    content = generate(template, args.mode, docling_image=args.docling_image)

    if args.output:
        output_path = args.output
    elif args.mode == "dev":
        workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY", ".")
        output_path = os.path.join(
            workspace, "deployment", "docker", "docker-compose.dev.yaml"
        )
    else:
        print(f"--output is required for --mode {args.mode}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(f"Written: {output_path}")


if __name__ == "__main__":
    main()
