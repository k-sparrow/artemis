from fastapi_healthchecks.checks import Check, CheckResult
from fastapi_healthchecks.checks.http import HttpCheck

__all__ = [
    "LivenessCheck",
    "StorageServiceHealthcheck",
    "IndexingServiceHealthcheck",
]


class LivenessCheck(Check):
    async def __call__(self) -> CheckResult:
        return CheckResult(name="Liveness", passed=True)


class StorageServiceHealthcheck(HttpCheck):
    def __init__(self, url: str, timeout: int = 10):
        super().__init__(
            url,
            username=None,
            password=None,
            verify_ssl=True,
            timeout=timeout,
            name="Readiness/Storage-Service",
        )


class IndexingServiceHealthcheck(HttpCheck):
    def __init__(self, url: str, timeout: int = 10):
        super().__init__(
            url,
            username=None,
            password=None,
            verify_ssl=True,
            timeout=timeout,
            name="Readiness/Indexing-Service",
        )
