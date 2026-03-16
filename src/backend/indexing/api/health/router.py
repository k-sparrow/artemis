from fastapi_healthchecks.api.router import HealthcheckRouter, Probe

from src.backend.indexing.api.config import settings
from src.backend.indexing.api.dependencies import (
    get_async_qdrant_client,
    get_embeddings_sync,
)
from src.backend.indexing.api.health.checks import (
    EmbeddingsHealthcheck,
    LivenessCheck,
    QdrantHealthcheck,
)


__all__ = [
    "router",
]


router = HealthcheckRouter(
    Probe(
        name="liveness",
        checks=[
            LivenessCheck(),
        ],
    ),
    Probe(
        name="readiness",
        checks=[
            EmbeddingsHealthcheck(
                embeddings=get_embeddings_sync(),
            ),
            QdrantHealthcheck(
                client=get_async_qdrant_client(),
                collection_name=settings.QDRANT_COLLECTION_NAME,
            ),
        ],
    ),
)
