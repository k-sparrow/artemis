# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#

from __future__ import annotations

from collections.abc import Sequence
import os
from typing import Any, Optional, List

from langchain_core.callbacks import Callbacks
from langchain_core.documents import Document, BaseDocumentCompressor
from langchain_core.utils import from_env
from pydantic import ConfigDict, Field, model_validator
from typing_extensions import Self

__all__ = [
    "HuggingFaceEndpointReranker",
]

DEFAULT_MODEL = "BAAI/bge-reranker-large"
VALID_TASKS = ("text-reranking",)


class HuggingFaceEndpointReranker(BaseDocumentCompressor):
    client: Any = None

    async_client: Any = None

    model: str | None = None
    """Model name to use."""

    provider: str | None = None
    """Name of the provider to use for inference with the model specified in
        `repo_id`. e.g. "sambanova". if not specified, defaults to HF Inference API.
        available providers can be found in the [huggingface_hub documentation](https://huggingface.co/docs/huggingface_hub/guides/inference#supported-providers-and-tasks)."""

    repo_id: str | None = None
    """Huggingfacehub repository id, for backward compatibility."""

    task: str | None = "text-reranking"
    """Task to call the model with."""

    model_kwargs: dict | None = None
    """Keyword arguments to pass to the model."""

    score_threshold: float = 0.0

    huggingfacehub_api_token: str | None = Field(
        default_factory=from_env("HUGGINGFACEHUB_API_TOKEN", default=None)
    )

    model_config = ConfigDict(
        extra="forbid",
        protected_namespaces=(),
    )

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        """Validate that api key and python package exists in environment."""
        huggingfacehub_api_token = self.huggingfacehub_api_token or os.getenv(
            "HF_TOKEN"
        )

        try:
            from src.lib.core.adapters.document_compressors.huggingface._client import (  # type: ignore[import]
                AsyncInferenceClient,
                InferenceClient,
            )

            if self.model:
                self.repo_id = self.model
            elif self.repo_id:
                self.model = self.repo_id
            else:
                self.model = DEFAULT_MODEL
                self.repo_id = DEFAULT_MODEL

            client = InferenceClient(
                model=self.model,
                token=huggingfacehub_api_token,
                provider=self.provider,  # type: ignore[arg-type]
            )

            async_client = AsyncInferenceClient(
                model=self.model,
                token=huggingfacehub_api_token,
                provider=self.provider,  # type: ignore[arg-type]
            )

            if self.task not in VALID_TASKS:
                msg = (
                    f"Got invalid task {self.task}, "
                    f"currently only {VALID_TASKS} are supported"
                )
                raise ValueError(msg)
            self.client = client
            self.async_client = async_client

        except ImportError as e:
            msg = (
                "Could not import huggingface_hub python package. "
                "Please install it with `pip install huggingface_hub`."
            )
            raise ImportError(msg) from e
        return self

    def compress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks=Optional[Callbacks],
    ) -> Sequence[Document]:
        """Rerank documents relative to the given query"""
        if not isinstance(documents, Sequence):
            raise ValueError("documents must be a list of Document objects")
        if not all(isinstance(d, Document) for d in documents):
            raise TypeError("All elements in documents must be instances of Document")

        # first extract the texts of the documents for reranking
        document_texts: List[str] = [d.page_content for d in documents]

        # ... then, rerank relative to user query
        _model_kwargs = self.model_kwargs or {}
        rankings = self.client.text_reranking(
            texts=document_texts, query=query, **_model_kwargs
        )

        # ... there is no need to sort the rankings, they are already sorted,
        # in descending order

        # ... and then, filter out the documents that have a "score" lower than the threshold
        score_threshold = _model_kwargs.get("score_threshold", 0.0)
        filtered_rankings = [r for r in rankings if r["score"] > score_threshold]

        # ... and finally, return the documents scored as relevant by the rerankers, in descending order
        reranked_docs = [documents[r["index"]] for r in filtered_rankings]

        return reranked_docs

    async def acompress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks=Optional[Callbacks],
    ) -> Sequence[Document]:
        """Rerank documents relative to the given query"""
        if not isinstance(documents, Sequence):
            raise ValueError("documents must be a list of Document objects")
        if not all(isinstance(d, Document) for d in documents):
            raise TypeError("All elements in documents must be instances of Document")

        document_texts: List[str] = [d.page_content for d in documents]

        # ... then, rerank relative to user query
        _model_kwargs = self.model_kwargs or {}
        rankings = await self.async_client.text_reranking(
            texts=document_texts, query=query, **_model_kwargs
        )

        # ... there is no need to sort the rankings, they are already sorted,
        # in descending order

        # ... and then, filter out the documents that have a "score" lower than the threshold
        score_threshold = _model_kwargs.get("score_threshold", 0.0)
        filtered_rankings = [r for r in rankings if r["score"] > score_threshold]

        # ... and finally, return the documents scored as relevant by the rerankers, in descending order
        reranked_docs = [documents[r["index"]] for r in filtered_rankings]

        return reranked_docs
