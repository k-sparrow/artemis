from fastapi import Request, responses, status


class IngestedFileNotFoundError(Exception):
    pass


class TaskNotFoundError(Exception):
    pass


def _ingested_file_not_found_handler(request: Request, exc: IngestedFileNotFoundError):
    return responses.JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": "File not found"},
    )


def _task_not_found_handler(request: Request, exc: TaskNotFoundError):
    return responses.JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": "Task not found"},
    )


EXCEPTION_HANDLER_MAP = {
    IngestedFileNotFoundError: _ingested_file_not_found_handler,
    TaskNotFoundError: _task_not_found_handler,
}
