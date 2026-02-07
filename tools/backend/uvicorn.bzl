"""
Uvicorn development and deployment server artifacts
Creates the following:

1. Bare bones developer server (Uvicorn)
2. Bare bones deployment server (Gunicorn)

These binaries can be used as a base for py_oci_image
which create container images
"""

# Python deps
load("@aspect_rules_py//py:defs.bzl", aspect_py_binary = "py_binary")
load("@pip//:requirements.bzl", "requirement")
load("@rules_python//python/entry_points:py_console_script_binary.bzl", "py_console_script_binary")

# macro for launching a development uvicorn server and gunicorn deployment server
# See here: https://www.uvicorn.org/deployment/
def uvicorn_server(*, name, app, **kwargs):
    """
    Bare bones development (Uvicorn) and deployment (Gunicorn) server.

    Args:
      name: name of the server target
      app: name of the FastAPI app path (pythonic path)
      **kwargs: key-value pairs
    """
    deps = kwargs.pop("deps", [])
    args = kwargs.pop("args", []) + [app]
    port = kwargs.pop("port", "9090")
    host = kwargs.pop("host", "localhost")

    uvicorn_args = [
        "--reload",
        "--host",
        host,
        "--port",
        port,
    ] + args

    gunicorn_args = [
        "--bind",
        "{ip}:{port}".format(ip = host, port = port),
        "-k",
        "uvicorn.workers.UvicornWorker",  # for ASGI, gunicorn is WSGI by default
    ] + args

    # Uvicorn development server (Bare-bones, a python binary)
    py_console_script_binary(
        name = name + ".dev",
        pkg = "@pip//uvicorn",
        script = "uvicorn",
        deps = [
            requirement("uvicorn"),
            requirement("starlette"),
        ] + deps,
        args = uvicorn_args,
        package_collisions = "ignore",  # ignore pacakge colissions
        binary_rule = aspect_py_binary,  # works better with rules_oci then rules_python
        **kwargs
    )

    # Gunicorn deployment server (Bare-bones, a python binary)
    py_console_script_binary(
        name = name,
        pkg = "@pip//gunicorn",
        script = "gunicorn",
        binary_rule = aspect_py_binary,
        package_collisions = "ignore",
        deps = [
            requirement("gunicorn"),
            requirement("uvicorn-worker"),
            requirement("uvicorn"),
            requirement("starlette"),
        ] + deps,
        args = gunicorn_args,
        **kwargs
    )
