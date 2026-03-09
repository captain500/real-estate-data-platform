"""Prefect task for reading data from the bronze layer (MinIO)."""

import polars as pl
from prefect import get_run_logger, task
from prefect.cache_policies import NONE

from real_estate_data_platform.connectors.minio import MinIOStorage


@task(cache_policy=NONE)
def read_bronze_listings(
    storage: MinIOStorage,
    source: str,
    city: str,
    partition_date: str,
) -> pl.DataFrame:
    """Read all Parquet files for a given partition from MinIO (bronze layer).

    Searches for Parquet files under the path:
        listings/source={source}/city={city}/dt={partition_date}/

    Args:
        storage: Pre-configured MinIOStorage client
        source: Data source name (e.g., 'kijiji')
        city: City name for partitioning (e.g., 'toronto')
        partition_date: Date string (YYYY-MM-DD)

    Returns:
        Polars DataFrame with all listings, or an empty DataFrame if none found
    """
    logger = get_run_logger()

    prefix = f"listings/source={source}/city={city}/dt={partition_date}/"
    parquet_files = storage.list_objects(prefix=prefix, suffix=".parquet")

    if not parquet_files:
        logger.warning("No parquet files found under %s", prefix)
        return pl.DataFrame()

    logger.info("Reading %d parquet file(s) from %s", len(parquet_files), prefix)

    frames = [storage.read_parquet(path) for path in parquet_files]
    df = pl.concat(frames)

    logger.info("Read %d rows from bronze layer", df.height)
    return df
