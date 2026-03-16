import json
from logging import Logger
from typing import Dict

import httpx
from httpx import QueryParams
from pydantic import ValidationError

# our code
from src.backend.controller.lib.schemas import (
    S3Details,
    SourceDetails,
    PrivateIngestionInfo,
    LibraryIngestionInfo,
    UploadAction,
    UpsertionMetaInfoType,
)
from src.backend.controller.worker.dependencies import get_s3_client


__all__ = [
    "metadata_prep",
    "upload_prep",
    "source_prep",
    "s3_to_ingestion",
]


def metadata_prep(
    upload_type: str,
    object_name: str,
    info: Dict[str, str],
) -> Dict[str, str]:
    match upload_type:
        case UpsertionMetaInfoType.LIBRARY:
            return {
                "namespace": f'library/{info["namespace"]}/{object_name}',
                "metadata": json.dumps(info),
            }
        case UpsertionMetaInfoType.PRIVATE:
            try:
                return {
                    "namespace": f'private/{info["chat_id"]}/{info["doc_id"]}',
                    "metadata": json.dumps(info),
                }
            except KeyError as e:
                raise ValidationError(
                    f"Missing required field in private metadata: {e}"
                )
        case _:
            raise ValueError(f"Unknown upload type: {upload_type}")


def upload_prep(
    upload_action: UploadAction,
    metadata: Dict[str, str],
) -> Dict[str, str]:
    """
    Prepares metadata for the upload request based on the upload type.
    """
    match upload_action:
        case UploadAction.CREATE | UploadAction.UPDATE:
            return {
                "namespace": metadata["namespace"],
                "metadata": metadata["metadata"],
            }
        case UploadAction.DELETE | UploadAction.AUTO_DELETE:
            return {
                "namespace": metadata["namespace"],
            }
        case _:
            raise ValueError(f"Unknown upload action: {upload_action}")


def source_prep(info: Dict[str, str]) -> str:
    """
    Prepares the source string for the upload request based on the info dictionary.
    """
    if "source" in info.keys():
        return info["source"]
    else:
        raise ValueError("Source information is missing in the provided info.")


# Utility function for ingestion tasks (must be after all imports and logger definition)
# Use string type annotations to avoid unbound errors
def s3_to_ingestion(
    s3: S3Details,
    source: SourceDetails,
    upload_action: UploadAction,
    info: PrivateIngestionInfo | LibraryIngestionInfo,
    ingestion_url: str,
    upsertion_type: UpsertionMetaInfoType,
    logger: Logger,
    logger_prefix: str = "",
) -> Dict[str, str]:
    """
    Loads an object from S3 and sends its bytes to the ingestion service.
    """
    minio_client = get_s3_client()
    try:
        response = minio_client.get_object(s3.bucket, s3.object)
        data = response.read()
    except Exception as e:
        logger.exception(f"{logger_prefix} Failed to load S3 object: {e}")
        raise e

    url = ingestion_url
    logger.info(
        f"{logger_prefix} Sending data to {url} with metadata: {info}, "
        f"operation is '{str(upload_action)}'"
    )

    metadata = metadata_prep(
        upload_type=upsertion_type,
        object_name=s3.object,
        info=info.model_dump(mode="json"),
    )

    logger.info(f"{logger_prefix} Metadata prepared: {metadata}")

    params = upload_prep(upload_action=upload_action, metadata=metadata)
    logger.info(
        f"{logger_prefix} Upload parameters for action '{str(upload_action)}' "
        f"prepared: {params}"
    )

    try:
        with httpx.Client() as client:
            match upload_action:
                case UploadAction.CREATE | UploadAction.UPDATE:
                    logger.info(f"{logger_prefix} Creating document {source}")
                    resp = client.post(
                        url,
                        files={"file": (source.path, data, source.content_type)},
                        data=params,
                        timeout=100000000,
                    )
                case UploadAction.DELETE | UploadAction.AUTO_DELETE:
                    logger.info(f"{logger_prefix} Deleting document {source}")
                    resp = client.delete(
                        url,
                        params=QueryParams(**params),
                    )
            resp.raise_for_status()

        logger.info(
            f"{logger_prefix} Finished, cleaning object {s3.object} "
            f"from bucket: {s3.bucket}"
        )
        minio_client.remove_object(s3.bucket, s3.object)

        logger.info(
            f"{logger_prefix} Response from {url}: {resp.text} "
            f"for action '{str(upload_action)}'"
            f"on object '{s3.object}' of file {source.path}"
        )

        return {"result": resp.text}
    except httpx.HTTPStatusError as e:
        logger.exception(
            f"{logger_prefix} HTTP error occurred: "
            f"{e.response.status_code} - {e.response.text}"
        )
        raise e
    except Exception as e:
        logger.exception(f"{logger_prefix} Failed to send data: {e}")
        raise e
