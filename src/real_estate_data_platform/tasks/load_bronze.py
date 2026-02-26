"""Prefect tasks for storage operations (MinIO, etc)."""

from datetime import UTC, date, datetime

import polars as pl
from prefect import get_run_logger, task

from real_estate_data_platform.config.settings import Environment
from real_estate_data_platform.connectors.minio import MinIOStorage
from real_estate_data_platform.models.enums import OperationStatus, ScraperMode
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import StorageResult


@task
def save_listings_to_minio(
    listings: list[RentalsListing],
    source: str,
    city: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    partition_date: str,
    max_pages: int,
    mode: ScraperMode,
    days: int,
    specific_date: date | None = None,
    environment: str = Environment.DEV.value,
    bucket_name: str = "raw",
) -> StorageResult:
    """Save listings to MinIO as Parquet with JSON metadata.

    1. Saves Parquet file with path: {bucket_name}/listings/source={source}/city={city}/dt={date}/listings_{YYYYMMDD}.parquet
    2. Saves metadata JSON with path: {bucket_name}/listings/source={source}/city={city}/dt={date}/_metadata.json

    Args:
        listings: List of RentalsListing objects
        source: Data source name (e.g., 'kijiji')
        city: City name for partitioning
        minio_endpoint: MinIO endpoint URL (e.g., 'minio:9000')
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
        partition_date: Date string for partition (YYYY-MM-DD)
        max_pages: Maximum number of pages scraped
        mode: ScraperMode used (last_x_days or specific_date)
        days: Number of days if mode is last_x_days
        specific_date: Specific date if mode is specific_date
        environment: Application environment ('dev' or 'prod'). Default: 'dev'
        bucket_name: S3 bucket name. Default: 'raw'

    Returns:
        StorageResult with metadata about saved files and operation status
    """
    task_logger = get_run_logger()

    try:
        # Initialize storage backend
        storage = MinIOStorage(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket_name=bucket_name,
            secure=(environment == Environment.PROD.value),
        )

        # Prepare file paths
        datestamp = partition_date.replace("-", "")
        base_dir = f"listings/source={source}/city={city}/dt={partition_date}"
        parquet_path = f"{base_dir}/listings_{datestamp}.parquet"
        metadata_path = f"{base_dir}/_metadata.json"

        # Convert RentalsListing objects to Polars DataFrame
        data = [listing.model_dump() for listing in listings]
        df = pl.DataFrame(data)

        # Save Parquet file
        storage.save_parquet(
            dataframe=df,
            object_name=parquet_path,
        )

        # Build metadata
        metadata_dict = {
            "mode": mode.value,
            "days": days,
            "specific_date": specific_date if specific_date else None,
            "max_pages": max_pages,
            "record_count": len(listings),
            "saved_at": datetime.now(UTC),
        }

        # Save metadata JSON
        storage.save_json(
            data=metadata_dict,
            object_name=metadata_path,
        )

        task_logger.info(f"Successfully saved {len(listings)} listings to {parquet_path}")
        return StorageResult(
            status=OperationStatus.SUCCESS,
            path=f"{bucket_name}/{parquet_path}",
            count=len(listings),
        )

    except Exception as e:
        task_logger.error(f"Error saving listings to MinIO: {e}")
        raise
