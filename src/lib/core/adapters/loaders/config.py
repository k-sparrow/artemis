# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Loader configuration and factory.

Mirrors the pipeline config/factory pattern: a serialisable
:class:`LoaderConfig` fully describes which loader to use and any
loader-specific parameters.  :func:`create_loader_factory` converts it
into a ``LoaderFactory`` callable ready to be used by the ingestion
service.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Callable

from langchain_core.document_loaders import BaseLoader

__all__ = [
    "LoaderType",
    "LoaderConfig",
    "create_loader_factory",
]

LoaderFactory = Callable[[bytes, str, str], BaseLoader]


class LoaderType(StrEnum):
    """Selects the document loader implementation."""

    PYMUPDF4LLM = "pymupdf4llm"


@dataclass(frozen=True)
class LoaderConfig:
    """Serialisable loader specification.

    Attributes:
        loader_type: Which loader implementation to use.
    """

    loader_type: LoaderType


def create_loader_factory(config: LoaderConfig) -> LoaderFactory:
    """Return a ``LoaderFactory`` configured according to *config*.

    The returned callable accepts ``(file: bytes, filename: str,
    content_type: str)`` and returns a ready-to-use
    :class:`~langchain_core.document_loaders.BaseLoader`.

    Args:
        config: Serialisable loader specification.

    Raises:
        ValueError: If ``config.loader_type`` is not a recognised value.
    """
    match config.loader_type:
        case LoaderType.PYMUPDF4LLM:
            from src.lib.core.adapters.loaders.pymupdf4llm_ import PyMuPDF4LLMLoader

            def factory(file: bytes, filename: str, content_type: str) -> BaseLoader:
                return PyMuPDF4LLMLoader(
                    file=file, filename=filename, content_type=content_type
                )

            return factory
        case _:
            raise ValueError(f"Unknown loader type: {config.loader_type!r}")
