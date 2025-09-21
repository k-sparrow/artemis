from typing import Dict

from fastapi import FastAPI

from src.backend.storage.api.private.exceptions import (
    EXCEPTION_HANDLER_MAP as PRIVATE_EXCEPTION_HANDLER_MAP,
)

__all__ = ["EXCEPTION_HANDLER_MAP", "register_custom_exception_handlers"]


EXCEPTION_HANDLER_MAP = {
    **PRIVATE_EXCEPTION_HANDLER_MAP,
}


def register_custom_exception_handlers(app: FastAPI, excpetion_handler_mapping: Dict):
    """
    Register custom exceptions handlers converting the exceptions to the corresponding
    HTTP responses
    """
    for ExpClass, exception_handler in excpetion_handler_mapping.items():
        app.add_exception_handler(ExpClass, exception_handler)
