import httpx
from fastapi_healthchecks.checks import Check, CheckResult
from fastapi_healthchecks.checks.http import HttpCheck
from qdrant_client import AsyncQdrantClient


__all__ = [
    "DoclingServeHealthcheck",
    "LivenessCheck",
    "OpenAICompatibleHealthcheck",
    "QdrantHealthcheck",
    "TeiHealthcheck",
]


class LivenessCheck(Check):
    """Simple liveness check that always passes."""

    async def __call__(self) -> CheckResult:
        return CheckResult(name="Liveness", passed=True)


class QdrantHealthcheck(Check):
    """Health check for Qdrant vector database.

    Verifies:
    1. Qdrant connection is alive
    2. Collection exists
    3. Schema has the namespace partition index
         (metadata.namespace_id with is_tenant=True)
    """

    def __init__(self, client: AsyncQdrantClient, collection_name: str):
        super().__init__()
        self._client = client
        self._collection_name = collection_name
        self._name = "Readiness/Qdrant"

    async def __call__(self) -> CheckResult:
        try:
            # Check 1: Connection alive
            if not await self._client.collection_exists(self._collection_name):
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details=f"Collection '{self._collection_name}' does not exist",
                )

            # Check 2 & 3: Get collection info and verify schema
            collection_info = await self._client.get_collection(self._collection_name)

            # Check for namespace partition index
            payload_schema = collection_info.payload_schema
            namespace_field = payload_schema.get("metadata.namespace_id")

            if namespace_field is None:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details="Missing 'metadata.namespace_id' index in collection schema",
                )

            # Verify it's a tenant index (UUID type with is_tenant=True)
            field_params = namespace_field.params
            if field_params is None:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details="'metadata.namespace_id' index has no params configured",
                )

            # Check if is_tenant is set
            is_tenant = getattr(field_params, "is_tenant", False)
            if not is_tenant:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details=(
                        "'metadata.namespace_id' index is not configured "
                        "as tenant partition"
                    ),
                )

            return CheckResult(
                name=self._name,
                passed=True,
                details="Qdrant vectorstore is ready",
            )

        except Exception as e:
            return CheckResult(
                name=self._name,
                passed=False,
                details=f"Qdrant connection error: {str(e)}",
            )


class TeiHealthcheck(Check):
    """Verifies TEI is up and serving an embedding model via GET /info."""

    def __init__(self, tei_url: str):
        super().__init__()
        self._info_url = tei_url.rstrip("/") + "/info"
        self._client = httpx.AsyncClient(timeout=5.0)
        self._name = "Readiness/TEI"

    async def __call__(self) -> CheckResult:
        try:
            resp = await self._client.get(self._info_url)
            resp.raise_for_status()
            info = resp.json()

            model_type = info.get("model_type", {})
            if "embedding" not in model_type:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details=(
                        f"TEI is not serving an embedding model"
                        f" (model_type={model_type})"
                    ),
                )

            return CheckResult(
                name=self._name,
                passed=True,
                details=f"TEI ready: model_id={info.get('model_id')}",
            )
        except Exception as e:
            return CheckResult(
                name=self._name,
                passed=False,
                details=f"TEI connection error: {e}",
            )


class OpenAICompatibleHealthcheck(Check):
    """
    Verifies an OpenAI-compatible server is up and serving the
    expected model via GET /v1/models.
    """

    def __init__(self, url: str, model_name: str, name: str):
        super().__init__()
        self._models_url = url.rstrip("/") + "/v1/models"
        self._model_name = model_name
        self._client = httpx.AsyncClient(timeout=5.0)
        self._name = name

    async def __call__(self) -> CheckResult:
        try:
            resp = await self._client.get(self._models_url)
            resp.raise_for_status()
            models = [m["id"] for m in resp.json().get("data", [])]

            if self._model_name not in models:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details=f"Model '{self._model_name}' not found (served: {models})",
                )

            return CheckResult(
                name=self._name,
                passed=True,
                details=f"Ready: model_id={self._model_name}",
            )
        except Exception as e:
            return CheckResult(
                name=self._name,
                passed=False,
                details=f"Connection error: {e}",
            )


class DoclingServeHealthcheck(HttpCheck):
    def __init__(self, url, timeout=60):
        super().__init__(
            url,
            username=None,
            password=None,
            verify_ssl=True,
            timeout=timeout,
            name="Readiness/Docling Serve",
        )
