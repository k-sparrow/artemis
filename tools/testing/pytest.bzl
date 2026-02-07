"""
pytest wrapper

Use as:

```
load("@//tools/testing:pytest.bzl", py_test = pytest_test)

py_test(
    name = ...
)
```
"""

load("@pip//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("@rules_python//python/entry_points:py_console_script_binary.bzl", "py_console_script_binary")

def pytest_test(name, srcs, **kwargs):
    """
    A pytest wrapper acting as py_test targets. Actually runs `pytest` console script.

    Args:
          name: name of the test
          srcs: list of source files
          **kwargs: key-value pairs of arguments
    """

    # this is only needed for passing srcs
    deps = kwargs.pop("deps", [])
    data = kwargs.pop("data", [])
    env = kwargs.pop("env", {})

    py_library(
        name = name + ".lib",
        srcs = srcs,
        deps = deps,
        data = data,
        testonly = True,
        **kwargs
    )

    # main test entry
    py_console_script_binary(
        name = name,
        pkg = "@pip//pytest:pkg",  # assuming your hub repo name is `pypi`.
        script = "pytest",
        binary_rule = py_test,
        deps = [
            # The test sources are here
            name + ".lib",
            # Add extra test deps below, e.g. for sharding support, etc.
            requirement("pytest-asyncio"),
        ],
        data = data + srcs,
        env = env,
        testonly = True,
        # The following is reusing the ideas defined in
        # https://github.com/caseyduquettesc/rules_python_pytest/blob/main/python_pytest/defs.bzl
        args = kwargs.get("args", ["--capture=no", "-v", "--asyncio-mode=auto"]) + [
            # Passing of srcs is optional and this is only to show how one
            # would reimplement what `rules_python_pytest` has done.
            "$(location :%s)" % x
            for x in srcs
        ],
        **kwargs
    )
