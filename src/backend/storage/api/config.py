from pydantic_settings import BaseSettings

__all__ = [
    "StorageSettings",
    "LOGGING_CONFIG",
]


class StorageSettings(BaseSettings):
    # S3 configuration
    S3_ENDPOINT_URL: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str

    # Venus bucket settings
    S3_VENUS_BUCKET: str
    S3_VENUS_BUCKET_KAFKA_EVENT: str


storage_settings = StorageSettings()


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {  # root logger
            "level": "INFO",  # "INFO",
            "handlers": ["default"],
            "propagate": False,
        },
        "uvicorn.error": {
            "level": "DEBUG",
            "handlers": ["default"],
        },
        "uvicorn.access": {
            "level": "DEBUG",
            "handlers": ["default"],
        },
    },
}
