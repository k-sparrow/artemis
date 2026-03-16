from fastapi_healthchecks.checks import Check, CheckResult
from fastapi_healthchecks.checks.http import HttpCheck

__all__ = [
    "DoclingServeHealthcheck",
    "LivenessCheck",
]


class LivenessCheck(Check):
    async def __call__(self) -> CheckResult:
        return CheckResult(name="Liveness", passed=True)


class DoclingServeHealthcheck(HttpCheck):
    def __init__(self, url: str, timeout: int = 60):
        super().__init__(
            url,
            username=None,
            password=None,
            verify_ssl=True,
            timeout=timeout,
            name="Readiness/Docling-Serve",
        )
