# -------------------------------------
# Copyright (c) 2025, Dror Kabely
# -------------------------------------
#

"""A macro for Celery worker instantiation"""

load("@aspect_rules_py//py:defs.bzl", aspect_py_binary = "py_binary")
load("@pip//:requirements.bzl", "requirement")
load("@rules_python//python/entry_points:py_console_script_binary.bzl", "py_console_script_binary")

def celery_worker(*, name, broker, **kwargs):
    if broker != "redis":
        fail("Unsupported broker type {broker}. Only Redis is supported currently.".format(broker = broker))

    # instantiate a py console script binary for the celery worker
    py_console_script_binary(
        name = name,
        pkg = requirement("celery"),
        script = "celery",
        deps = [
            requirement("redis"),
        ] + kwargs.pop("deps", []),
        binary_rule = aspect_py_binary,  # fits better with rules_oci
        **kwargs
    )
