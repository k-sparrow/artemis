from fastapi_healthchecks.api.router import HealthcheckRouter, Probe

from src.backend.parsing.api.config import settings
from src.backend.parsing.api.health.checks import DoclingServeHealthcheck, LivenessCheck

__all__ = ["router"]


router = HealthcheckRouter(
    Probe(
        name="liveness",
        checks=[LivenessCheck()],
    ),
    Probe(
        name="readiness",
        checks=[
            DoclingServeHealthcheck(url=settings.DOCLING_SERVE_HEALTHCHECK_URL),
        ],
    ),
)
