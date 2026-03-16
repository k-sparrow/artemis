from typing import Annotated

from fastapi import Depends

from src.lib.core.adapters.loaders import (
    LoaderConfig,
    LoaderFactory,
    LoaderType,
    create_loader_factory,
)
from src.backend.parsing.api.config import settings

__all__ = [
    "LoaderFactory",
    "loader_factory_dependency",
]


def get_loader_factory(
    loader_type: LoaderType = settings.LOADER_TYPE,
) -> LoaderFactory:
    config = LoaderConfig(
        loader_type=loader_type,
        docling_base_url=settings.DOCLING_SERVE_URI,
    )
    return create_loader_factory(config)


loader_factory_dependency = Annotated[LoaderFactory, Depends(get_loader_factory)]
