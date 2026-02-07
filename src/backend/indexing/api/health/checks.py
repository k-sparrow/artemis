from fastapi_healthchecks.checks import Check, CheckResult
from langchain_core.embeddings import Embeddings
from qdrant_client import AsyncQdrantClient

from fastapi_healthchecks.checks.http import HttpCheck


__all__ = [
    "DoclingServeHealthcheck",
    "EmbeddingsHealthcheck",
    "LivenessCheck",
    "QdrantHealthcheck",
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
    3. Schema has the namespace partition index (metadata.namespace with is_tenant=True)
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
            namespace_field = payload_schema.get("metadata.namespace")

            if namespace_field is None:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details="Missing 'metadata.namespace' index in collection schema",
                )

            # Verify it's a tenant index (UUID type with is_tenant=True)
            field_params = namespace_field.params
            if field_params is None:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details="'metadata.namespace' index has no params configured",
                )

            # Check if is_tenant is set
            is_tenant = getattr(field_params, "is_tenant", False)
            if not is_tenant:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details="'metadata.namespace' index is not configured as tenant partition",
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


class EmbeddingsHealthcheck(Check):
    """
    Health check for embeddings.

    Verifies:
    1. Embeddings are alive
    2. Embedding dims are greater than 0
    """

    def __init__(self, embeddings: Embeddings):
        super().__init__()
        self._embeddings = embeddings
        self._name = "Readiness/Embeddings"

    async def __call__(self) -> CheckResult:
        try:
            result = await self._embeddings.aembed_query("dummy")
            dim = len(result)
            if dim <= 0:
                return CheckResult(
                    name=self._name,
                    passed=False,
                    details=f"Received empty embeddings vector",
                )
            return CheckResult(
                name=self._name,
                passed=True,
                details=f"Embeddings ready",
            )
        except Exception as e:
            return CheckResult(
                name=self._name,
                passed=False,
                details=f"Embeddings connection error: {str(e)}",
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
