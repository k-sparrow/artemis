# Development Guide

## Prerequisites

- **Bazel** — build system; install via [Bazelisk](https://github.com/bazelbuild/bazelisk)
- **Docker** (with Compose v2) — for running infrastructure and services
- **Python 3.11+** — for running services locally outside Docker
- **NVIDIA GPU + CUDA 12.8** — required only if running Docling Serve locally

---

## Key Bazel Commands

```bash
# Update Python dependencies (edit requirements.in first, then run this)
bazel run //:requirements.update

# Format code (ruff)
bazel run //:format

# Run health checks (requirements consistency, gazelle manifest, format)
bazel test //:health

# Update gazelle manifest after dependency changes
bazel run //:gazelle_python_manifest.update

# Regenerate docker-compose files from the template
bazel run //deployment/docker:docker_compose.update

# Check compose files are in sync with the template (CI drift check)
bazel test //deployment/docker:docker_compose.update_tests
```

**Never run bare `bazel run //:gazelle`** — BUILD files are hand-maintained. Only
`gazelle_python_manifest.update` should be run, to update the gazelle manifest after
dependency changes.

---

## Adding Python Dependencies

1. Edit `requirements.in` to add the new package
2. Run `bazel run //:requirements.update` to regenerate `requirements_lock.txt`
3. Add the dependency to the relevant `BUILD.bazel` `deps` list
4. Run `bazel run //:gazelle_python_manifest.update`

---

## Adding New Python Files

After creating a new `.py` file in an existing package, update the gazelle manifest:

```bash
bazel run //:gazelle_python_manifest.update
```

Do **not** run `bazel run //:gazelle` — it would overwrite hand-maintained BUILD files.

---

## Running Services Locally (Outside Docker)

Start infrastructure first (PostgreSQL, MinIO, Qdrant, Kafka, RabbitMQ):

```bash
docker compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra up -d
```

Then run individual services:

```bash
# Storage service (port 7000)
uvicorn src.backend.storage.api.main:app --host 0.0.0.0 --port 7000

# Indexing service (port 10000)
uvicorn src.backend.indexing.api.main:app --host 0.0.0.0 --port 10000

# Parsing service (port 10001)
uvicorn src.backend.parsing.api.main:app --host 0.0.0.0 --port 10001

# Enterprise intake (port 9000)
uvicorn src.backend.enterprise.intake.api.main:app --host 0.0.0.0 --port 9000

# Enterprise data sources (port 9500)
uvicorn src.backend.enterprise.data_sources.api.main:app --host 0.0.0.0 --port 9500

# MCP server (port 11000)
uvicorn src.backend.mcp.api.main:app --host 0.0.0.0 --port 11000

# Controller worker (both queues)
celery -A src.backend.controller.worker.celery worker \
  --queues gpu_bound,io_bound --loglevel=info
```

---

## Running the Test Suite

Tests are organised into layers (see `CLAUDE.md` for the full strategy):

```bash
# Unit tests only (fast, no infra — runs in CI on every PR)
pytest -m "unit"

# Unit + integration (testcontainers — ~2 min)
pytest -m "unit or integration"

# Kafka topology tests (slow, separate suite)
pytest -m "kafka"

# E2E tests (full compose stack — nightly or pre-release)
pytest -m "e2e"
```

Integration and E2E tests use [testcontainers-python](https://testcontainers-python.readthedocs.io/)
to spin up real Docker containers. They require Docker to be running.

Tests tagged `local` require Docker volume mounts to host paths — they cannot run under the
Bazel Linux sandbox. Run them with `pytest` directly (not via `bazel test`).

---

## Important Bazel Gotcha: `__init__.py` Wiping

Broad `bazel test` runs (not `//:format`) can silently empty `__init__.py` files under the
Linux sandbox. Always run `git diff` before committing after any wide test run:

```bash
git diff --stat
```

If `__init__.py` files show as changed (empty), restore them:

```bash
git restore src/  # or the specific files
```

---

## Adding a New Service

1. Create `src/backend/<service>/api/` with `main.py`, `config.py`, `router.py`
2. Add a `BUILD.bazel` in the service directory
3. Run `bazel run //:gazelle_python_manifest.update`
4. Add a `py_image` target in `tools/oci/images/<service>/BUILD.bazel`
5. Add the service to `tools/docker/docker-compose.tmpl.yaml` under the appropriate profile
6. Run `bazel run //deployment/docker:docker_compose.update` to regenerate compose files
7. Add health check and unit tests

---

## Docker Image Versioning

Images are stamped with a version via Bazel `--stamp` (see `tools/stamping/stamped_tags.bzl`).
The version comes from `STABLE_VERSION` in the workspace status command (typically `git describe`).

Build a release image:
```bash
# Must be at a clean tagged commit (git describe must return a clean tag)
bazel build //tools/oci/images/<service>:image --stamp
```

The release docker-compose uses `${ARTEMIS_VERSION}` which must be set to the same tag.
