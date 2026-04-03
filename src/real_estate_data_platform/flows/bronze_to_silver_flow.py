"""Prefect flow for moving data from Bronze to Silver layer."""

from datetime import date

from prefect import flow, get_run_logger

from real_estate_data_platform.config.settings import Environment, settings
from real_estate_data_platform.connectors.minio import MinIOStorage
from real_estate_data_platform.connectors.postgres import PostgresStorage
from real_estate_data_platform.models.enums import City, DataSource, DateMode, FlowStatus
from real_estate_data_platform.models.responses import BronzeToSilverResult, PartitionResult
from real_estate_data_platform.models.silver_schema import (
    LISTING_COLUMNS,
    LISTINGS_REGISTRY,
    NEIGHBOURHOOD_COLUMNS,
    NEIGHBOURHOOD_REGISTRY,
    build_create_table_sql,
    build_listings_upsert_sql,
    build_neighbourhood_upsert_sql,
)
from real_estate_data_platform.tasks.load_silver import write_silver
from real_estate_data_platform.tasks.read_bronze import read_bronze_listings
from real_estate_data_platform.tasks.transform_silver import transform_to_silver
from real_estate_data_platform.utils.dates import date_range, format_date


@flow(name="process-partition", timeout_seconds=600)
def process_partition(
    source: str,
    city: str,
    partition_date: str,
) -> PartitionResult:
    """Read, transform and load a single bronze partition into silver.

    Each invocation is a separate Prefect flow run.
    This isolation allows for better error handling and observability at the partition level.
    """
    logger = get_run_logger()
    logger.info("Processing partition: %s/%s/%s", source, city, partition_date)

    partition = {"source": source, "city": city, "partition_date": partition_date}

    # Initialise connectors
    try:
        minio_storage = MinIOStorage(
            endpoint=settings.minio.endpoint,
            access_key=settings.minio.access_key,
            secret_key=settings.minio.secret_key.get_secret_value(),
            bucket_name=settings.minio.bucket_name,
            secure=(settings.environment == Environment.PROD),
        )

        pg_cfg = settings.postgres
        schema = pg_cfg.silver_schema
        auto_create = settings.environment != Environment.PROD

        pg_listings = PostgresStorage(
            dsn=pg_cfg.dsn,
            schema=schema,
            table=pg_cfg.silver_listings_table,
            upsert_sql=build_listings_upsert_sql(schema=schema, table=pg_cfg.silver_listings_table),
            columns=LISTING_COLUMNS,
            create_table_sql=build_create_table_sql(
                LISTINGS_REGISTRY,
                schema=schema,
                table=pg_cfg.silver_listings_table,
            ),
            auto_create_schema=auto_create,
        )

        pg_neighbourhoods = PostgresStorage(
            dsn=pg_cfg.dsn,
            schema=schema,
            table=pg_cfg.silver_neighbourhoods_table,
            upsert_sql=build_neighbourhood_upsert_sql(
                schema=schema,
                table=pg_cfg.silver_neighbourhoods_table,
            ),
            columns=NEIGHBOURHOOD_COLUMNS,
            create_table_sql=build_create_table_sql(
                NEIGHBOURHOOD_REGISTRY,
                schema=schema,
                table=pg_cfg.silver_neighbourhoods_table,
            ),
            auto_create_schema=auto_create,
        )
    except Exception as exc:
        logger.error("Connector init failed: %s", exc)
        return PartitionResult(status=FlowStatus.ERROR, **partition, error=f"Connector init: {exc}")

    # Read bronze data, transform to silver, and load into PostgreSQL
    with pg_listings, pg_neighbourhoods:
        df_bronze = read_bronze_listings(
            storage=minio_storage,
            source=source,
            city=city,
            partition_date=partition_date,
        )
        if df_bronze.is_empty():
            return PartitionResult(status=FlowStatus.COMPLETED_NO_DATA, **partition)

        rows_read = df_bronze.height

        try:
            silver = transform_to_silver(df_bronze)
            rows_loaded = write_silver(pg=pg_listings, df=silver.listings)
            neighbourhoods_loaded = write_silver(pg=pg_neighbourhoods, df=silver.neighbourhoods)
        except Exception:
            logger.error("ETL failed for %s/%s/%s", source, city, partition_date, exc_info=True)
            return PartitionResult(
                status=FlowStatus.ERROR,
                **partition,
                rows_read=rows_read,
                error="ETL failed",
            )

    return PartitionResult(
        status=FlowStatus.SUCCESS,
        **partition,
        rows_read=rows_read,
        rows_loaded=rows_loaded,
        neighbourhoods_loaded=neighbourhoods_loaded,
    )


@flow(
    name="bronze-to-silver",
    timeout_seconds=1800,
    log_prints=True,
)
def bronze_to_silver(
    source: DataSource = DataSource.KIJIJI,
    city: City | None = None,
    mode: DateMode = DateMode.LAST_X_DAYS,
    days: int = 1,
    specific_date: date | None = None,
) -> BronzeToSilverResult:
    """Main flow to move data from bronze (MinIO) to silver (PostgreSQL).

    For each (source, city, date) partition, reads raw listings from MinIO,
    transforms to silver schema, and upserts into PostgreSQL. Each partition is
    processed in a separate subflow for better isolation and observability.

    Args:
        source: Data source to process.
        city: City to process. If ``None``, all cities are processed.
        mode: DateMode to use (``last_x_days`` or ``specific_date``).
        days: Number of days to look back (1 = today only). Used when mode is ``LAST_X_DAYS``.
        specific_date: Specific partition date. Required when mode is ``SPECIFIC_DATE``.

    Returns:
        BronzeToSilverResult with per-partition detail.
    """
    logger = get_run_logger()

    # Validate parameters
    if mode == DateMode.SPECIFIC_DATE and not specific_date:
        error_msg = "specific_date is required when mode is SPECIFIC_DATE"
        logger.error(error_msg)
        return BronzeToSilverResult(status=FlowStatus.ERROR, error=error_msg)

    cities = [city] if city else list(City)
    dates = [format_date(specific_date)] if mode == DateMode.SPECIFIC_DATE else date_range(days)

    logger.info(
        "Starting bronze to silver flow: source=%s, cities=%s, dates=%s",
        source.value,
        [c.value for c in cities],
        dates,
    )

    # Dispatch one subflow per partition
    results = [
        process_partition(source=source.value, city=c.value, partition_date=dt)
        for dt in dates
        for c in cities
    ]

    summary = BronzeToSilverResult.from_partitions(results)
    logger.info(
        "Bronze to Silver complete: %d read, %d loaded, %d ok / %d no-data / %d error — status=%s",
        summary.total_read,
        summary.total_loaded,
        summary.partitions_ok,
        summary.partitions_no_data,
        summary.partitions_error,
        summary.status,
    )
    return summary


if __name__ == "__main__":
    result = bronze_to_silver(
        source=DataSource.KIJIJI,
        city=City.TORONTO,
        mode=DateMode.LAST_X_DAYS,
        days=1,
    )
