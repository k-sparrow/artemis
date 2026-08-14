"""Integration test: the patched docling-serve image actually has the
Ray-serde fix live via a real import — not just present as a file somewhere
in the image.

An earlier version of the image (see //tools/oci/images/docling for the full
writeup) placed the patch tar layer under `.../lib64/...`, which looked
byte-for-byte correct as a build artifact but silently served the *unpatched*
file at runtime: `lib64` is a symlink to `lib` in the base image, and
`docker load` merging a layer's synthetic directory entry at that path
against the base's symlink shadows it with a sparse, incomplete tree instead
of traversing through it. A file-existence/content check at a path alone
can't catch that class of bug, only an actual import inside a real running
container does.

Prerequisite:
    bazel run //tools/oci/images/docling:tarball
"""

from __future__ import annotations

import docker
import pytest

_IMAGE = "artemis/docling-serve-ray-patched:latest"
_CHECK_SCRIPT = (
    "import docling_jobkit.orchestrators.ray.models as m\n"
    "import inspect\n"
    "src = inspect.getsource(m.SourceChunkConvertRequest)\n"
    "assert 'chunk: DocumentChunk = Field(' in src, f'patch not live:\\n{src}'\n"
    "print('OK: patch is live, imported from', m.__file__)\n"
)


@pytest.mark.integration
def test_ray_serde_patch_is_live() -> None:
    client = docker.from_env()
    try:
        output = client.containers.run(
            _IMAGE,
            ["python3", "-c", _CHECK_SCRIPT],
            entrypoint=[],
            remove=True,
            stdout=True,
            stderr=True,
        )
    except docker.errors.ContainerError as exc:
        pytest.fail(f"patch check failed inside the image: {exc.stderr.decode()}")
    assert b"OK: patch is live" in output, output.decode()
