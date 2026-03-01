"""Prefect tasks for storage operations (MinIO, etc)."""

from datetime import UTC, date, datetime

import polars as pl
from prefect import get_run_logger, task
from prefect.cache_policies import NONE

from real_estate_data_platform.connectors.minio import MinIOStorage
from real_estate_data_platform.models.enums import ScraperMode
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import ScrapeMetadata, StorageResult


@task(cache_policy=NONE)
def listings_to_dataframe(listings: list[RentalsListing]) -> pl.DataFrame:
    """Convert a list of RentalsListing objects to a Polars DataFrame.

    Args:
        listings: List of RentalsListing Pydantic models

    Returns:
        Polars DataFrame with one row per listing
    """
    logger = get_run_logger()
    data = [listing.model_dump() for listing in listings]
    df = pl.DataFrame(data)
    logger.info("Converted %d listings to DataFrame", df.height)
    return df


@task(cache_policy=NONE)
def save_listings_to_minio(
    df: pl.DataFrame,
    storage: MinIOStorage,
    source: str,
    city: str,
    partition_date: str,
    max_pages: int,
    mode: ScraperMode,
    days: int,
    specific_date: date | None = None,
) -> StorageResult:
    """Save a listings DataFrame to MinIO as Parquet with JSON metadata.

    Args:
        df: Polars DataFrame with listing data
        storage: MinIOStorage instance
        source: Data source name (e.g., 'kijiji')
        city: City name for partitioning
        partition_date: Date string for partition (YYYY-MM-DD)
        max_pages: Maximum number of pages scraped
        mode: ScraperMode used (last_x_days or specific_date)
        days: Number of days if mode is last_x_days
        specific_date: Specific date if mode is specific_date

    Returns:
        StorageResult with metadata about saved files
    """
    logger = get_run_logger()

    # Prepare file paths
    datestamp = partition_date.replace("-", "")
    base_dir = f"listings/source={source}/city={city}/dt={partition_date}"
    parquet_path = f"{base_dir}/listings_{datestamp}.parquet"
    metadata_path = f"{base_dir}/_metadata.json"

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
        record_count=df.height,
        saved_at=datetime.now(UTC),
    )

    # Save metadata JSON
    storage.save_json(
        data=metadata.model_dump(mode="json"),
        object_name=metadata_path,
    )

    logger.info("Successfully saved %d listings to %s", df.height, parquet_path)
    return StorageResult(
        path=f"{storage.bucket_name}/{parquet_path}",
        count=df.height,
    )
