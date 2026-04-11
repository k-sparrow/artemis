import asyncio
from fastapi_healthchecks.checks import Check, CheckResult
from kafka_connect import KafkaConnect

__all__ = [
    "KafkaConnectConnectorCheck",
]


class KafkaConnectConnectorCheck(Check):
    """Readiness check: HTTP sink connector must be in RUNNING state."""

    def __init__(
        self,
        name: str,
        kc_client: KafkaConnect,
        connector_name: str,
    ) -> None:
        super().__init__()
        self._name = name
        self._kc_client: KafkaConnect = kc_client
        self._connector_name: str = connector_name

    def _get_status(
        self,
    ) -> dict:
        return self._kc_client.get_connector_status(self._connector_name)

    async def __call__(self) -> CheckResult:
        try:
            status = await asyncio.to_thread(self._get_status)
        except Exception as exc:
            return CheckResult(name=self._name, passed=False, details=str(exc))

        tasks = status.get("tasks", [])
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        healthy = (
            connector_state == "RUNNING"
            and bool(tasks)
            and all(t["state"] == "RUNNING" for t in tasks)
        )
        return CheckResult(
            name=self._name,
            passed=healthy,
            details=f"connector={connector_state}, tasks={[t['state'] for t in tasks]}",
        )
