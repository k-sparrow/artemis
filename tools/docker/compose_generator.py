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
  {ARTEMIS_TAG_DEV}    Tag for artemis app images that ship a :dev variant
  {ARTEMIS_TAG_LATEST} Tag for artemis images that ship a :latest variant

Modes
-----
  dev      Local development. Artemis images at :dev/:latest; everything published.
  test     e2e via testcontainers. CPU Docling/TEI, no GPU mounts.
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
    "# WARNING: still carries the template's DEV secrets/passwords — override them\n"
    "# (JWT_SECRET, MinIO/Postgres creds) before any non-staging use.\n"
    "# ============================================================================\n"
)

_DEV_SUBS: dict[str, str] = {
    # Pinned to the latest stable release (NOT a moving :main tag): the
    # parsing-service redesign depends on the /v1/chunk/hybrid response shape
    # (per-chunk page_numbers + documents[].md_content). cu128 GPU variant.
    "DOCLING_IMAGE": "ghcr.io/docling-project/docling-serve-cu128:v1.24.0",
    "DOCLING_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
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
    "DOCLING_IMAGE": "ghcr.io/docling-project/docling-serve:main",
    "DOCLING_GPU_BLOCK": "",
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

# Release reuses the dev (GPU, prod-image) substitutions, only the artemis tags
# differ — both collapse to the pinned ${ARTEMIS_VERSION}.
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


def generate(template: str, mode: str) -> str:
    subs = _SUBS_BY_MODE[mode]
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
    args = parser.parse_args()

    with open(args.template, encoding="utf-8") as fh:
        template = fh.read()

    content = generate(template, args.mode)

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
