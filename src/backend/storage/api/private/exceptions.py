from fastapi import Request, responses, status

__all__ = [
    "FilesAndFileIdsLengthMismatch",
    "EXCEPTION_HANDLER_MAP",
]


class FilesAndFileIdsLengthMismatch(ValueError):
    def __init__(self, n_files: int, n_file_ids: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._number_of_files = n_files
        self._number_of_file_ids = n_file_ids

    def __str__(self):
        return (
            f"ValueError: mismatch between number of files ({self._number_of_files}) "
            f"and number of file IDs ({self._number_of_file_ids})"
        )


def files_and_file_ids_mismatch_handler(
    request: Request, exc: FilesAndFileIdsLengthMismatch
):
    return responses.JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"detail": str(exc)}
    )


EXCEPTION_HANDLER_MAP = {
    FilesAndFileIdsLengthMismatch: files_and_file_ids_mismatch_handler
}
