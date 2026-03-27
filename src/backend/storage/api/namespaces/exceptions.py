from fastapi import Request, responses, status


class InvalidOwnerIdError(ValueError):
    pass


class OnlyPrivateNamespaceCanBeRenamedError(ValueError):
    pass


def _invalid_owner_id_handler(request: Request, exc: InvalidOwnerIdError):
    return responses.JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": "Invalid X-Owner-Id header"},
    )


def _only_private_namespace_can_be_renamed_handler(
    request: Request, exc: OnlyPrivateNamespaceCanBeRenamedError
):
    return responses.JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"detail": "Only PRIVATE namespaces can be renamed"},
    )


EXCEPTION_HANDLER_MAP = {
    InvalidOwnerIdError: _invalid_owner_id_handler,
    OnlyPrivateNamespaceCanBeRenamedError: _only_private_namespace_can_be_renamed_handler,
}
