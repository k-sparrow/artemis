# core
from typing import Dict

# third party
from fastapi import FastAPI

__all__ = [
    "register_custom_exception_handlers",
]


def register_custom_exception_handlers(app: FastAPI, excpetion_handler_mapping: Dict):
    """
    Register custom exceptions handlers converting the exceptions to the corresponding
    HTTP responses
    """
    for ExpClass, exception_handler in excpetion_handler_mapping.items():
        app.add_exception_handler(ExpClass, exception_handler)
