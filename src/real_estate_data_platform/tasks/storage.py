"""Prefect tasks for storage operations (MinIO, etc)."""

import logging
from datetime import datetime
from io import BytesIO

import pandas as pd
from minio import Minio
from minio.api import ObjectWriteResult
from prefect import get_run_logger, task

from real_estate_data_platform.models.listings import RentalsListing

logger = logging.getLogger(__name__)


@task
def save_listings_to_minio(
    listings: list[RentalsListing],
    city: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    bucket_name: str = "raw",
    scrape_date: str = None,
) -> dict:
    """Save listings to MinIO in Parquet format with Hive partitioning structure.

    Stores files in: `{bucket_name}/TorontoRentals/city={city}/dt={date}/parquets/`

    Args:
        listings: List of RentalsListing objects
        city: City name for partitioning
        minio_endpoint: MinIO endpoint URL (e.g., 'minio:9000')
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
        bucket_name: S3 bucket name (default: 'raw')
        scrape_date: Date string for partition (YYYY-MM-DD). If None, uses today's date.

    Returns:
        Dict with metadata about saved files
    """
    task_logger = get_run_logger()

    if not listings:
        task_logger.warning("No listings to save")
        return {"status": "skipped", "reason": "empty_listings"}

    if scrape_date is None:
        scrape_date = datetime.utcnow().strftime("%Y-%m-%d")

    # Convert to DataFrame
    data = [listing.model_dump() for listing in listings]
    df = pd.DataFrame(data)

    task_logger.info(f"Converting {len(df)} listings to Parquet")

    # Save to BytesIO
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
    buffer.seek(0)

    # Connect to MinIO
    client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,  # Set to True if using HTTPS
    )

    # Create file path with Hive partitioning structure
    # raw/TorontoRentals/city=toronto/dt=2026-02-15/parquets/listings.parquet
    object_name = f"TorontoRentals/city={city.lower()}/dt={scrape_date}/parquets/listings.parquet"

    try:
        task_logger.info(f"Uploading to s3://{bucket_name}/{object_name}")

        result: ObjectWriteResult = client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            metadata={"Content-Type": "application/octet-stream"},
        )

        task_logger.info(f"Successfully uploaded {len(df)} listings to {object_name}")

        return {
            "status": "success",
            "bucket": bucket_name,
            "object_path": object_name,
            "listings_count": len(df),
            "etag": result.etag,
            "version_id": result.version_id,
        }

    except Exception as e:
        task_logger.error(f"Error uploading to MinIO: {e}")
        raise


def get_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    """Create MinIO client instance.

    Args:
        endpoint: MinIO endpoint
        access_key: Access key
        secret_key: Secret key

    Returns:
        Configured Minio client
    """
    # Remove http:// or https:// prefix if present
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
