from __future__ import annotations

from fastapi import APIRouter, status

from src.backend.enterprise.intake.api.dependencies import storage_client_dependency
from src.backend.enterprise.intake.api.intake import service
from src.backend.enterprise.intake.api.intake.schemas import (
    IntakeRequest,
    IntakeResponse,
)
from src.lib.backend.logging import get_logger


router = APIRouter(tags=["Intake"])
log = get_logger(__name__)


@router.post(
    "/intake",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IntakeResponse,
)
async def intake_endpoint(
    body: IntakeRequest, http: storage_client_dependency
) -> IntakeResponse:
    task_id, s3_key = await service.intake_file(http=http, request=body)
    log.info(
        "intake_dispatched",
        source_type=body.source.type,
        namespace_id=str(body.namespace_id),
        display_name=body.display_name,
        task_id=str(task_id),
    )
    return IntakeResponse(
        task_id=task_id, s3_key=s3_key, namespace_id=body.namespace_id
    )
