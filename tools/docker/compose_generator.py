#!/usr/bin/env python3
"""Generate docker-compose YAML for dev or test modes from a template file.

Usage:
    compose_generator --mode dev|test --template PATH [--output PATH]

The template file uses {PLACEHOLDER} markers:

  {DOCLING_IMAGE}      Full image ref for docling-serve
  {DOCLING_GPU_BLOCK}  Multi-line YAML block for nvidia runtime + deploy section;
                       empty string in test mode (CPU-only Docling)
  {TEI_IMAGE}          Full image ref for HuggingFace TEI
  {TEI_MODEL}          Model ID passed to TEI at startup
  {COLBERT_COMMAND_BLOCK}  Full YAML list-form command block for the ColBERT vLLM service
  {COLBERT_GPU_BLOCK}      Multi-line YAML block for nvidia runtime + deploy section

Bazel integration
-----------------
  bazel run  //tools/docker:docker_compose.update   # writes deployment/docker/docker-compose.dev.yaml
  bazel build //tools/docker:docker_compose_test    # produces bazel-bin artefact
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

_DEV_SUBS: dict[str, str] = {
    "DOCLING_IMAGE": "ghcr.io/docling-project/docling-serve-cu126:main",
    "DOCLING_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
    "TEI_IMAGE": "ghcr.io/huggingface/text-embeddings-inference:1.5",
    "TEI_MODEL": "BAAI/bge-large-en-v1.5",
    "COLBERT_COMMAND_BLOCK": (
        "\n"
        "    command:\n"
        "      - jinaai/jina-colbert-v2\n"
        "      - --pooler-config.task\n"
        "      - token_embed\n"
        "      - --hf-overrides\n"
        """      - '{"architectures": ["ColBERTJinaRobertaModel"]}'\n"""
        "      - --trust-remote-code"
    ),
    "COLBERT_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
}

_TEST_SUBS: dict[str, str] = {
    "DOCLING_IMAGE": "ghcr.io/docling-project/docling-serve:main",
    "DOCLING_GPU_BLOCK": "",
    "TEI_IMAGE": "ghcr.io/huggingface/text-embeddings-inference:cpu-1.5",
    "TEI_MODEL": "BAAI/bge-small-en-v1.5",
    "COLBERT_COMMAND_BLOCK": (
        "\n"
        "    command:\n"
        "      - colbert-ir/colbertv2.0\n"
        "      - --pooler-config.task\n"
        "      - token_embed"
    ),
    "COLBERT_GPU_BLOCK": _GPU_DEPLOY_BLOCK,
}


def generate(template: str, mode: str) -> str:
    subs = _DEV_SUBS if mode == "dev" else _TEST_SUBS
    result = template
    for key, value in subs.items():
        result = result.replace("{" + key + "}", value)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["dev", "test"],
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
        print("--output is required for --mode test", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(f"Written: {output_path}")


if __name__ == "__main__":
    main()
