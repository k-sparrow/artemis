from minio import Minio
from minio.error import MinioException
from minio.notificationconfig import NotificationConfig, QueueConfig


__all__ = [
    "create_bucket",
    "link_s3_bucket_with_kafka_event",
]


# TODO: move this to a utility module
def format_kafka_event_arn_sqs(main_name: str) -> str:
    return f"arn:minio:sqs::{main_name}:kafka"


# Create a new bucket if it does not exist
async def create_bucket(minio_client: Minio, bucket_name: str) -> None:
    try:
        if not minio_client.bucket_exists(bucket_name=bucket_name):
            minio_client.make_bucket(bucket_name=bucket_name, object_lock=False)
    except MinioException:
        raise


async def link_s3_bucket_with_kafka_event(
    minio_client: Minio, bucket_name: str, event_name_name: str
) -> None:
    try:
        # link between the bucket and the event topic
        minio_client.set_bucket_notification(
            bucket_name=bucket_name,
            config=NotificationConfig(
                queue_config_list=[
                    QueueConfig(
                        queue=format_kafka_event_arn_sqs(main_name=event_name_name),
                        # This will emit only s3:ObjectCreated:Put for some reason
                        # but s3:ObjectCreated:Put will set the event type to "other"
                        # (also for unknown reason)
                        events=["s3:ObjectCreated:*"],
                    )
                ]
            ),
        )
    except MinioException:
        raise
