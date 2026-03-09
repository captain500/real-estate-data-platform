"""Prefect task for writing data to the silver layer (PostgreSQL)."""

import polars as pl
from prefect import task
from prefect.cache_policies import NONE

from real_estate_data_platform.connectors.postgres import PostgresStorage


@task(cache_policy=NONE)
def write_silver_listings(
    pg: PostgresStorage,
    df: pl.DataFrame,
) -> int:
    """Upsert a silver DataFrame into PostgreSQL.

    Args:
        pg: Pre-configured PostgresStorage client
        df: Cleaned Polars DataFrame (output of transform_to_silver)

    Returns:
        Number of rows upserted
    """
    return pg.upsert(df)
