# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

from typing import Any, List, Optional

from fastapi import APIRouter
from langchain_core.documents import Document
from langchain_core.runnables import RunnableSerializable
from langchain_core.runnables.config import RunnableConfig
from langserve import add_routes

from src.backend.indexing.api.retrieve import service as retrieve_service
from src.backend.indexing.api.retrieve.utils import pre_req_config_modifier


__all__ = [
    "router",
]


class _LazyRetriever(RunnableSerializable):
    """Placeholder that satisfies LangServe at import time, but defers the
    real singleton lookup until a request arrives.

    ``add_routes`` executes at module import time — before FastAPI's lifespan
    runs.  It inspects the runnable immediately to generate JSON schemas and
    register routes.  Passing ``retrieve_service.get_retriever()`` directly
    would raise a ``RuntimeError`` at that point because the vectorstore
    connection, embeddings model, and retrieval adapter are not yet
    initialised.

    This class is a valid ``RunnableSerializable`` (correct types, correct
    interface) so LangServe is satisfied at import time.  Each ``invoke``
    call simply forwards to the real singleton, which is guaranteed to be
    populated by the time the first request arrives.
    """

    def invoke(
        self, input: str, config: Optional[RunnableConfig] = None, **kwargs: Any
    ) -> List[Document]:
        return retrieve_service.get_retriever().invoke(input, config=config, **kwargs)


router = APIRouter(prefix="/retrieve", tags=["Retrieval"])

add_routes(
    app=router,
    runnable=_LazyRetriever().with_types(input_type=str, output_type=List[Document]),
    per_req_config_modifier=pre_req_config_modifier,
    disabled_endpoints=["batch"],
)
