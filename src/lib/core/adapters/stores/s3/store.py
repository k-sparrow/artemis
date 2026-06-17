# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""S3-backed LangChain :class:`ByteStore`, native-async via aiobotocore.

aiobotocore speaks plain S3, so this one adapter works against MinIO, Ceph RGW,
or AWS S3 — the store is server-agnostic. The **async** methods
(``amget``/``amset``/``amdelete``/``ayield_keys``) are implemented natively, so
``MultiVectorRetriever.amget`` and ``StoreDocumentIndex``'s async upsert/delete
hit real coroutines — no ``run_in_executor`` seam, and aiohttp's connector pools
connections cleanly (unlike minio-py's repeated-read quirk).

``StoreDocumentIndex`` wraps this with ``create_kv_docstore`` to serialise
Documents ↔ bytes, and ``MultiVectorRetriever`` reads it the same way, so the
write key and the read key are guaranteed identical.

The sync methods are intentionally unimplemented: this store is async-only.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence

import aiobotocore.session
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError
from langchain_core.stores import ByteStore

__all__ = ["S3ByteStore"]


class S3ByteStore(ByteStore):
    """A LangChain ``ByteStore`` (``BaseStore[str, bytes]``) over one S3 bucket."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
    ) -> None:
        self._session = aiobotocore.session.get_session()
        self._bucket = bucket
        self._client_kwargs = dict(
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            # Path-style is mandatory for MinIO/Ceph (virtual-host addressing
            # resolves bucket-in-host, which they don't serve).
            config=AioConfig(s3={"addressing_style": "path"}),
        )

    def _client(self):
        # A fresh aiohttp connector per op. Holding one client open in the app
        # lifespan is a possible optimization if the per-call build profiles as
        # significant; not required.
        return self._session.create_client("s3", **self._client_kwargs)

    async def ensure_bucket(self) -> None:
        async with self._client() as c:
            try:
                await c.head_bucket(Bucket=self._bucket)
            except ClientError:
                await c.create_bucket(Bucket=self._bucket)

    # --- async-native ByteStore API ------------------------------------------

    async def amget(self, keys: Sequence[str]) -> list[bytes | None]:
        async with self._client() as c:

            async def _one(key: str) -> bytes | None:
                try:
                    resp = await c.get_object(Bucket=self._bucket, Key=key)
                except ClientError as exc:
                    if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                        return None  # callers (MultiVectorRetriever) expect None
                    raise
                async with resp["Body"] as body:
                    return await body.read()

            return list(await asyncio.gather(*(_one(k) for k in keys)))

    async def amset(self, key_value_pairs: Sequence[tuple[str, bytes]]) -> None:
        async with self._client() as c:
            await asyncio.gather(
                *(
                    c.put_object(Bucket=self._bucket, Key=k, Body=v)
                    for k, v in key_value_pairs
                )
            )

    async def amdelete(self, keys: Sequence[str]) -> None:
        async with self._client() as c:
            await asyncio.gather(
                *(c.delete_object(Bucket=self._bucket, Key=k) for k in keys)
            )

    async def ayield_keys(self, *, prefix: str | None = None) -> AsyncIterator[str]:
        async with self._client() as c:
            paginator = c.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self._bucket, Prefix=prefix or ""
            ):
                for obj in page.get("Contents", []):
                    yield obj["Key"]

    # --- sync API: this store is async-only ----------------------------------

    def mget(self, keys: Sequence[str]) -> list[bytes | None]:
        raise NotImplementedError("S3ByteStore is async-only; use amget")

    def mset(self, key_value_pairs: Sequence[tuple[str, bytes]]) -> None:
        raise NotImplementedError("S3ByteStore is async-only; use amset")

    def mdelete(self, keys: Sequence[str]) -> None:
        raise NotImplementedError("S3ByteStore is async-only; use amdelete")

    def yield_keys(self, *, prefix: str | None = None) -> Iterator[str]:
        raise NotImplementedError("S3ByteStore is async-only; use ayield_keys")
