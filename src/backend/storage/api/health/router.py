from fastapi_healthchecks.api.router import HealthcheckRouter, Probe

from src.backend.storage.api.config import storage_settings as settings
from src.backend.storage.api.dependencies import get_minio_client_sync
from src.backend.storage.api.health.checks import MinioHealthcheck

__all__ = [
    "router",
]


router = HealthcheckRouter(
    Probe(
        name="readiness",
        checks=[
            MinioHealthcheck(
                client=get_minio_client_sync(),
                bucket_name=settings.S3_VENUS_BUCKET,
            ),
        ],
    )
)
