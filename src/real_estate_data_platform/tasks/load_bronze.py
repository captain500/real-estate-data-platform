"""Prefect tasks for storage operations (MinIO, etc)."""

import logging

from prefect import get_run_logger, task

from real_estate_data_platform.config.settings import Environment
from real_estate_data_platform.connectors.minio import MinIOStorage
from real_estate_data_platform.models.enums import OperationStatus
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import StorageResult

logger = logging.getLogger(__name__)


@task
def save_listings_to_minio(
    listings: list[RentalsListing],
    source: str,
    city: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    environment: str = Environment.DEV.value,
    bucket_name: str = "raw",
    partition_date: str | None = None,
) -> StorageResult:
    """Save listings to MinIO as Parquet with partitioning.

    Stores files with structure:
    `s3://{bucket_name}/listings/source={source}/city={city}/dt={date}/data.parquet`

    Args:
        listings: List of RentalsListing objects
        source: Data source name (e.g., 'kijiji')
        city: City name for partitioning
        minio_endpoint: MinIO endpoint URL (e.g., 'minio:9000')
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
        environment: Application environment ('dev' or 'prod'). Default: 'dev'
        bucket_name: S3 bucket name. Default: 'raw'
        partition_date: Date string for partition (YYYY-MM-DD).
                       If None, uses today's date.

    Returns:
        StorageResult with metadata about saved files and operation status
    """
    task_logger = get_run_logger()

    if not listings:
        task_logger.warning("No listings to save")
        from datetime import datetime

        return StorageResult(
            status=OperationStatus.SKIPPED,
            reason="empty_listings",
            count=0,
            timestamp=datetime.now().strftime("%Y-%m-%d"),
        )

    try:
        # Initialize storage backend
        storage = MinIOStorage(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket_name=bucket_name,
            secure=(environment == Environment.PROD.value),
        )

        # Save listings
        result = storage.save_listings(
            listings=listings,
            source=source,
            city=city,
            partition_date=partition_date,
        )

        if result.status == OperationStatus.SUCCESS:
            task_logger.info(f"Successfully saved {result.count} listings to {result.path}")
        else:
            task_logger.error(f"Failed to save listings: {result.reason}")

        return result

    except Exception as e:
        task_logger.error(f"Unexpected error saving listings: {e}")
        raise
