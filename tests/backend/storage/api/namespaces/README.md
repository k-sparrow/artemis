# Unit Tests — Storage Namespaces Router

Tests for `GET/POST/PATCH/DELETE /namespaces` endpoints.
Source under test: `src/backend/storage/api/namespaces/router.py`.

---

## What these tests cover

| Concern | Covered |
|---|---|
| Router parses request body and headers correctly | Yes |
| Router calls the right service function with the right arguments | Yes |
| Router maps service return value → correct response shape and status code | Yes |
| Exception → HTTP status code mapping (handler registration wiring) | Yes |
| Pydantic schema validation rules | Yes |
| Service business logic (DB queries, S3 operations) | **No** — service is mocked out |
| Real DB transactions, constraints, ORM behaviour | **No** — covered by integration tests |

---

## Fixture layering

```
tests/backend/storage/api/conftest.py            ← session-wide, autouse
tests/backend/storage/api/namespaces/conftest.py ← suite-level
tests/backend/storage/api/namespaces/test_namespaces.py
```

### Layer 1 — `api/conftest.py` (autouse, applies to every test)

**Environment variables**

```python
os.environ.setdefault("S3_ENDPOINT_URL", "localhost:9000")
os.environ.setdefault("SQL_DB_URL", "postgresql+asyncpg://test:test@localhost/test")
# ... other vars
```

These are set at **module import time**, before any settings object is instantiated.
`StorageSettings` is a `pydantic_settings.BaseSettings` — it reads env vars when the
class is first instantiated, which happens on import of `config.py`. If the vars are
not set before that import, Pydantic raises a `ValidationError` and the entire test
collection fails.

**Lifespan mock**

```python
@pytest.fixture(autouse=True)
def mock_lifespan(monkeypatch):
    monkeypatch.setattr(app.router, "lifespan_context", _noop)
```

`FastAPI` stores the lifespan on `app.router.lifespan_context` at construction time
(i.e. at `main.py` import time). `TestClient.__enter__()` starts the ASGI app, which
calls `app.router.lifespan_context`.

Patching `utils.lifespan` after the fact has **no effect** because `app` already holds
a direct reference to the original function. The correct target is
`app.router.lifespan_context` — the attribute FastAPI actually dispatches through at
startup. Replacing it with a no-op prevents the real lifespan from running, which
would otherwise try to connect to MinIO and Postgres (neither of which exists in unit
tests).

**Bare stubs**

```python
@pytest.fixture
def mock_session() -> AsyncMock:
    return AsyncMock()

@pytest.fixture
def mock_minio() -> MagicMock:
    return MagicMock()
```

These are plain stubs with no behaviour attached. Behaviour is added per-test via
`patch`. They are defined here so the files conftest can inject them into the
`client` fixture without knowing the test details.

---

### Layer 2 — `namespaces/conftest.py`

```python
@pytest.fixture
def client(mock_session, mock_minio) -> TestClient:
    app.dependency_overrides[get_db_session] = lambda: mock_session
    app.dependency_overrides[get_minio_client] = lambda: mock_minio
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
```

`FastAPI.dependency_overrides` is a dict mapping a dependency callable to a
replacement. When the router declares `session: db_session_dependency`, FastAPI
resolves the `Depends(get_db_session)` annotation. At request time it checks
`dependency_overrides` first — finding `get_db_session` mapped to
`lambda: mock_session`, it calls the lambda and injects the `AsyncMock` instead of
opening a real database session.

The `with TestClient(app) as c:` block is important: entering the context manager
starts the ASGI app (triggering the now-mocked lifespan); exiting it shuts it down
cleanly.

`dependency_overrides.clear()` after the `yield` is not optional. `app` is a
**module-level singleton** shared across all tests in the process. A leaked override
would silently corrupt any test that runs afterward.

`mock_session` and `mock_minio` are also available as standalone fixtures so
individual tests can receive them and assert on how they were called.

---

### Layer 3 — the test file

#### `_namespace()` — the fixture helper

```python
def _namespace(type_=PRIVATE, name="test-ns") -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), name=name, type=type_, ...)
```

The router calls `NamespaceResponse.model_validate(namespace)` on whatever the
service returns. `NamespaceResponse` has `model_config = {"from_attributes": True}`,
which means Pydantic reads fields by attribute access (`obj.id`, `obj.name`, …)
rather than by dict key. `SimpleNamespace` satisfies this — it is a plain Python
object with arbitrary attributes.

SQLAlchemy model instances created via `__new__` do **not** work here. The ORM
instrumentation (`InstrumentedAttribute.impl`) is only initialised when an instance
is constructed through the mapper; bypassing it via `__new__` leaves `impl = None`,
and any attribute assignment raises `AttributeError: 'NoneType' object has no
attribute 'set'`.

---

## How the pieces interact during a test

### Happy-path test

```python
def test_private_namespace_returns_201(self, client):
    ns = _namespace(NamespaceType.PRIVATE)
    with patch(f"{_SERVICE}.create_namespace", new=AsyncMock(return_value=ns)):
        response = client.post("/namespaces", json={"type": "private"},
                               headers={"X-Owner-Id": OWNER_ID})
    assert response.status_code == 201
    assert response.json()["type"] == "private"
```

Execution path:

1. `client.post(...)` sends an HTTP request through the ASGI test transport — no
   network socket is involved.
2. FastAPI routes it to `create_namespace_endpoint`.
3. The endpoint declares `session: db_session_dependency` → resolved to `mock_session`
   via `dependency_overrides`.
4. `service.parse_owner_id(owner_id)` is **not** patched, so the real function runs
   (it just parses a UUID string).
5. `await service.create_namespace(session=mock_session, ...)` **is** patched at the
   module attribute level. The `AsyncMock` is called instead and returns the
   `SimpleNamespace`.
6. The router calls `NamespaceResponse.model_validate(ns)` → produces a valid
   response dict.
7. FastAPI serialises it to JSON and returns 201.

**Why `patch(f"{_SERVICE}.create_namespace")`?**

`_SERVICE = "src.backend.storage.api.namespaces.service"`. This patches the name
`create_namespace` on the service module object at runtime. The router imports the
module (`from src.backend.storage.api.namespaces import service`) and calls
`service.create_namespace(...)`, which looks up the name on the module at call time —
the patch intercepts that lookup.

If the router had instead done `from service import create_namespace` and called
`create_namespace(...)` directly, the patch target would need to be the router's own
module namespace, not the service module.

### Error-path test

```python
def test_not_found_returns_404(self, client):
    with patch(f"{_SERVICE}.get_namespace",
               new=AsyncMock(side_effect=NamespaceNotFoundError())):
        response = client.get(f"/namespaces/{uuid.uuid4()}")
    assert response.status_code == 404
```

1. The mocked `get_namespace` raises `NamespaceNotFoundError` instead of returning a
   value.
2. FastAPI's exception handler mechanism catches it. `register_custom_exception_handlers`
   registered `NamespaceNotFoundError → _namespace_not_found_handler` on the app at
   import time (in `main.py`).
3. The handler returns `JSONResponse(status_code=404, ...)`.
4. The test asserts 404.

This verifies not that `NamespaceNotFoundError` exists, but that the
**router-to-exception-handler wiring** is correct end-to-end. A missing or
mis-registered handler would let the exception propagate as a 500, which the
assertion would catch.

### Test that patches nothing

```python
def test_shared_without_name_returns_422(self, client):
    response = client.post("/namespaces", json={"type": "shared"},
                           headers={"X-Owner-Id": OWNER_ID})
    assert response.status_code == 422
```

No patch is needed. The `NamespaceCreate` model validator runs inside FastAPI's
request parsing phase, before the endpoint function body is ever entered. FastAPI
catches Pydantic's `ValidationError` and returns 422 automatically — the service is
never called. This test verifies that the validation rule lives in the correct place
(the schema layer, not the service layer).

---

## What to look at next

- **`tests/backend/storage/api/files/`** — same structure, same patterns, for the
  ingestion and observability endpoints.
- **`tests/backend/storage/api/integration/`** — Tier 2 tests that exercise the full
  HTTP → service → real Postgres + MinIO path. No service mocks. Those tests cover
  the business logic and persistence behaviour that unit tests deliberately skip.
