from typing import Annotated

import httpx
from fastapi import Depends
from kafka_connect import KafkaConnect

from src.backend.enterprise.intake.api.config import settings

storage_client: httpx.AsyncClient = httpx.AsyncClient(
    base_url=settings.STORAGE_SERVICE_URL
)

kafka_connect_client: KafkaConnect = KafkaConnect(url=settings.KAFKA_CONNECT_URL)


async def get_storage_client() -> httpx.AsyncClient:
    return storage_client


async def get_kafka_connect_client() -> KafkaConnect:
    return kafka_connect_client


storage_client_dependency = Annotated[httpx.AsyncClient, Depends(get_storage_client)]
kafka_connect_dependency = Annotated[KafkaConnect, Depends(get_kafka_connect_client)]
