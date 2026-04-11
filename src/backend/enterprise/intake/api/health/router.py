from fastapi_healthchecks.api.router import HealthcheckRouter, Probe
from fastapi_healthchecks.checks.http import HttpCheck

from src.backend.enterprise.intake.api.config import settings
from src.backend.enterprise.intake.api.dependencies import kafka_connect_client
from src.backend.enterprise.intake.api.health.checks import KafkaConnectConnectorCheck

__all__ = [
    "router",
]


router = HealthcheckRouter(
    Probe(
        name="liveness",
        checks=[],
    ),
    Probe(
        name="readiness",
        checks=[
            HttpCheck(
                url=f"{settings.STORAGE_SERVICE_URL}/health/readiness",
                name="StorageService",
            ),
            HttpCheck(
                url=f"{settings.KAFKA_CONNECT_URL}/health",
                name="KafkaConnect",
            ),
            KafkaConnectConnectorCheck(
                kc_client=kafka_connect_client,
                connector_name=settings.HTTP_SINK_CONNECTOR_NAME,
                name="HttpSinkConnector",
            ),
        ],
    ),
)
