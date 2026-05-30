from fastapi_healthchecks.api.router import HealthcheckRouter, Probe

from src.backend.mcp.api.health.checks import (
    IndexingServiceHealthcheck,
    LivenessCheck,
    StorageServiceHealthcheck,
)
from src.backend.mcp.api.settings import settings

__all__ = ["router"]

router = HealthcheckRouter(
    Probe(name="liveness", checks=[LivenessCheck()]),
    Probe(
        name="readiness",
        checks=[
            StorageServiceHealthcheck(
                url=f"{settings.STORAGE_SERVICE_URL}/health/liveness"
            ),
            IndexingServiceHealthcheck(
                url=f"{settings.INDEXING_SERVICE_URL}/health/liveness"
            ),
        ],
    ),
)
