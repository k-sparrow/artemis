from __future__ import annotations

import httpx

__all__ = ["VLLMLateInteractionEmbeddings"]

_TIMEOUT = 60.0


class VLLMLateInteractionEmbeddings:
    """ColBERT late-interaction embeddings via vLLM's /pooling endpoint.

    The vLLM server must be started with `--pooler-config.task token_embed`.
    Each input is encoded as a token matrix (list[list[float]]) rather than
    a single pooled vector.

    For dev/test Docker Compose, Infinity (`michaelf34/infinity`) can be
    substituted — its /v1/embeddings response uses the same matrix shape
    but under the `embedding` key instead of `data`.
    """

    def __init__(self, url: str, model: str) -> None:
        self._url = url.rstrip("/") + "/pooling"
        self._model = model

    def embed_documents(self, texts: list[str]) -> list[list[list[float]]]:
        response = httpx.post(
            self._url,
            json={"model": self._model, "input": texts},
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        data = sorted(response.json()["data"], key=lambda x: x["index"])
        return [item["data"] for item in data]

    def embed_query(self, text: str) -> list[list[float]]:
        return self.embed_documents([text])[0]

    async def aembed_documents(self, texts: list[str]) -> list[list[list[float]]]:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.post(
                self._url,
                json={"model": self._model, "input": texts},
            )
        response.raise_for_status()
        data = sorted(response.json()["data"], key=lambda x: x["index"])
        return [item["data"] for item in data]

    async def aembed_query(self, text: str) -> list[list[float]]:
        return (await self.aembed_documents([text]))[0]
