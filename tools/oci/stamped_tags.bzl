"""Stamped `repo_tags` files for oci_load / oci_push.

`oci_load`'s `repo_tags` accepts either a string list or a label to a file with
one `repository:tag` per line (rules_oci 1.8.0). This macro generates that file
so an image carries a release version stamped from git alongside the static dev
tag the compose stack references.

- Default (`--nostamp`): the version line stays `<repository>:0.0.0` — a clearly
  non-release placeholder; the dev tag(s) are unaffected.
- Release (`bazel run … --stamp`): `0.0.0` is rewritten to the `STABLE_VERSION`
  workspace-status value (see tools/workspace_status.sh → `git describe --tags`),
  e.g. `<repository>:v1.0.0-alpha.1`.

Usage:
    load("//tools/oci:stamped_tags.bzl", "stamped_tags")

    stamped_tags(name = "tags", repository = "artemis/backend-storage")  # dev_tags = ["dev"]
    oci_load(name = "tarball.dev", image = ":image", repo_tags = ":tags")
"""

load("@aspect_bazel_lib//lib:expand_template.bzl", "expand_template")

# Placeholder rewritten to STABLE_VERSION under --stamp. Matches the rules_oci
# stamp_tags example so the pattern is recognisable.
_VERSION_PLACEHOLDER = "0.0.0"

def stamped_tags(name, repository, dev_tags = ["dev"], **kwargs):
    """Generate a repo_tags file: static dev tag(s) + a git-stamped version tag.

    Args:
        name: target name; pass `:<name>` to `oci_load(repo_tags = ...)`.
        repository: image repository, e.g. "artemis/backend-storage".
        dev_tags: static tags always emitted (kept for the dev compose stack).
        **kwargs: forwarded to expand_template (e.g. visibility).
    """
    expand_template(
        name = name,
        out = "_{}.tags.txt".format(name),
        template = [repository + ":" + t for t in dev_tags] +
                   [repository + ":" + _VERSION_PLACEHOLDER],
        stamp_substitutions = {_VERSION_PLACEHOLDER: "{{STABLE_VERSION}}"},
        **kwargs
    )
