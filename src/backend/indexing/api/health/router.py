from fastapi_healthchecks.api.router import HealthcheckRouter, Probe

from src.backend.indexing.api.config import settings
from src.backend.indexing.api.dependencies import get_async_qdrant_client
from src.backend.indexing.api.health.checks import (
    LivenessCheck,
    OpenAICompatibleHealthcheck,
    QdrantHealthcheck,
    TeiHealthcheck,
)


__all__ = [
    "router",
]

_readiness_checks = [
    TeiHealthcheck(tei_url=settings.TEI_HOST_URL),
    QdrantHealthcheck(
        client=get_async_qdrant_client(),
        collection_name=settings.QDRANT_COLLECTION_NAME,
    ),
]

if settings.COLBERT_RERANKER_URL:
    _readiness_checks.append(
        OpenAICompatibleHealthcheck(
            url=settings.COLBERT_RERANKER_URL,
            model_name=settings.COLBERT_MODEL_NAME,
            name="Readiness/ColBERT",
        )
    )

router = HealthcheckRouter(
    Probe(name="liveness", checks=[LivenessCheck()]),
    Probe(name="readiness", checks=_readiness_checks),
)
