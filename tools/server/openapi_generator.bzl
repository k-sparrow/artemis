# -------------------------------------
# Copyright (c) 2025, Dror Kabely
# -------------------------------------
#

"""A macro for generating the openapi client for a given spec and config."""

load("@pip//:requirements.bzl", "requirement")
load("@rules_python//python/entry_points:py_console_script_binary.bzl", "py_console_script_binary")

def openapi_client_generator(*, name, spec, config, **kwargs):
    py_console_script_binary(
        name = name,
        pkg = requirement("openapi-python-client"),
        script = "openapi-python-client",
        args = [
            "generate",
            "--path",
            "$(location {spec})".format(spec = spec),
            "--config",
            "$(location {config})".format(config = config),
            "--overwrite",
        ] + kwargs.pop("args", []),
        data = [
            spec,
            config,
        ] + kwargs.pop("data", []),
        deps = kwargs.pop("deps", []),
        **kwargs
    )
