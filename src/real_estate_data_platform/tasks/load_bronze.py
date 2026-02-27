"""Prefect tasks for storage operations (MinIO, etc)."""

from datetime import UTC, date, datetime

import polars as pl
from prefect import get_run_logger, task

from real_estate_data_platform.connectors.minio import MinIOStorage
from real_estate_data_platform.models.enums import OperationStatus, ScraperMode
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import ScrapeMetadata, StorageResult


@task
def save_listings_to_minio(
    listings: list[RentalsListing],
    storage: MinIOStorage,
    source: str,
    city: str,
    partition_date: str,
    max_pages: int,
    mode: ScraperMode,
    days: int,
    specific_date: date | None = None,
) -> StorageResult:
    """Save listings to MinIO as Parquet with JSON metadata.

    Args:
        listings: List of RentalsListing objects
        storage: Pre-configured MinIOStorage instance
        source: Data source name (e.g., 'kijiji')
        city: City name for partitioning
        partition_date: Date string for partition (YYYY-MM-DD)
        max_pages: Maximum number of pages scraped
        mode: ScraperMode used (last_x_days or specific_date)
        days: Number of days if mode is last_x_days
        specific_date: Specific date if mode is specific_date

    Returns:
        StorageResult with metadata about saved files and operation status
    """
    logger = get_run_logger()

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

    # Build and save metadata
    metadata = ScrapeMetadata(
        mode=mode,
        days=days,
        specific_date=specific_date,
        max_pages=max_pages,
        record_count=len(listings),
        saved_at=datetime.now(UTC),
    )

    # Save metadata JSON
    storage.save_json(
        data=metadata.model_dump(mode="json"),
        object_name=metadata_path,
    )

    logger.info("Successfully saved %d listings to %s", len(listings), parquet_path)
    return StorageResult(
        status=OperationStatus.SUCCESS,
        path=f"{storage.bucket_name}/{parquet_path}",
        count=len(listings),
    )
