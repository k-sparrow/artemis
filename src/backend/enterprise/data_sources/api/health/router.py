from fastapi_healthchecks.api.router import HealthcheckRouter, Probe
from fastapi_healthchecks.checks.http import HttpCheck

from src.backend.enterprise.data_sources.api.config import settings

__all__ = ["router"]

router = HealthcheckRouter(
    Probe(name="liveness", checks=[]),
    Probe(
        name="readiness",
        checks=[
            HttpCheck(
                url=f"{settings.STORAGE_SERVICE_URL}/health/readiness",
                name="StorageService",
            ),
            HttpCheck(
                url=f"{settings.KAFKA_CONNECT_URL}/",
                name="KafkaConnect",
            ),
        ],
    ),
)
