from pydantic import computed_field
from pydantic_settings import BaseSettings

from src.lib.core.adapters.loaders import LoaderType

__all__ = [
    "ParsingSettings",
    "settings",
]


class ParsingSettings(BaseSettings):
    DEBUG: bool = False
    OTEL_EXPORTER_OTLP_ENDPOINT: str = ""
    DOCLING_SERVE_URI: str
    LOADER_TYPE: LoaderType = LoaderType.DOCLING

    # MinIO S3 client — used to read input by reference and write parse
    # artifacts (claim-check). Required (no defaults) so a misconfigured
    # deployment fails fast at startup rather than silently building an
    # unreachable client.
    S3_ENDPOINT: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str
    S3_SECURE: bool

    # Bucket where parse artifacts are written for the indexing service to read.
    PARSED_ARTIFACTS_BUCKET: str = "parsed-chunks"

    # Private bucket for the lossless DoclingDocument replay cache. Never part of
    # the parse→index contract — insurance for re-chunk / future citations
    # without a GPU re-parse.
    REPLAY_CACHE_BUCKET: str = "docling-replay"

    # Per-call HTTP timeouts for the new async parse endpoints. Resolve and chunk
    # finalize no longer call docling-serve at all (Epic 21 §21.9 — results are
    # discovered via S3 listing instead of an inline HTTP fetch), so there is no
    # DOCLING_RESOLVE_TIMEOUT/DOCLING_FINALIZE_TIMEOUT HTTP call left to bound.
    DOCLING_STATUS_TIMEOUT: float = 30.0

    @computed_field
    @property
    def DOCLING_SERVE_HEALTHCHECK_URL(self) -> str:
        return f"{self.DOCLING_SERVE_URI.rstrip('/')}/health"


settings = ParsingSettings()
