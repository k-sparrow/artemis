#!/usr/bin/env bash
# Bazel workspace status command. Emits "stable" keys (prefix STABLE_) that
# invalidate stamped actions when they change. Wired in .bazelrc via
# `build --workspace_status_command`. Only baked into outputs under `--stamp`
# (opt-in for releases); normal `--nostamp` dev builds ignore these values.
set -euo pipefail

# Release version derived from the current git tag (e.g. v1.0.0-alpha.1).
# --dirty marks an uncommitted tree so a release can't be cut from one silently.
echo "STABLE_VERSION $(git describe --tags --always --dirty 2>/dev/null || echo 0.0.0-unknown)"
