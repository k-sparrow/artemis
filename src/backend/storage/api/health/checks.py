from fastapi_healthchecks.checks import Check, CheckResult
from minio import Minio


__all__ = ["MinioHealthcheck"]


class MinioHealthcheck(Check):
    def __init__(self, client: Minio, bucket_name: str):
        super().__init__()

        self._client = client
        self._bucket_name = bucket_name

    async def __call__(self) -> CheckResult:
        if self._client.bucket_exists(self._bucket_name):
            return CheckResult(
                name="Minio",
                passed=True,
            )
        else:
            return CheckResult(
                name="Minio",
                passed=False,
                details=(
                    f"Bucket {self._bucket_name} does not exist in S3 storage "
                    f"at '{self._client._base_url}'"
                ),
            )
