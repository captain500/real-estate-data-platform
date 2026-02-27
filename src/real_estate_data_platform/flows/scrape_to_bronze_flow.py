"""Prefect flow for scraping rental listings."""

from datetime import UTC, date, datetime

from prefect import flow, get_run_logger

from real_estate_data_platform.config.settings import Environment, settings
from real_estate_data_platform.connectors.minio import MinIOStorage
from real_estate_data_platform.models.enums import City, FlowStatus, ScraperMode
from real_estate_data_platform.models.responses import ScrapeToBronzeResult
from real_estate_data_platform.scrapers.scraper_type import ScraperType
from real_estate_data_platform.tasks.load_bronze import save_listings_to_minio
from real_estate_data_platform.tasks.scraping import (
    aggregate_results,
    fetch_and_parse_page,
)
from real_estate_data_platform.utils.dates import format_date


@flow(name="scrape-to-bronze")
def scrape_to_bronze(
    scraper_type: ScraperType,
    city: City = City.TORONTO,
    mode: ScraperMode = ScraperMode.LAST_X_DAYS,
    days: int = 7,
    specific_date: date | None = None,
    max_pages: int = 10,
) -> ScrapeToBronzeResult:
    """Scrape listings from a rental website and save to MinIO raw bucket.

    This flow:
    1. Instantiates the specified scraper type
    2. Fetches multiple pages in parallel
    3. Aggregates results
    4. Saves to MinIO in partitioned Parquet format

    Args:
        scraper_type: ScraperType enum specifying which scraper to use
        city: City to scrape (City enum)
        mode: ScraperMode to use (last_x_days or specific_date)
        days: Number of days for last_x_days mode
        specific_date: Specific date for specific_date mode
        max_pages: Maximum number of pages to scrape

    Returns:
        ScrapeToBronzeResult with scraping metadata and results
    """
    logger = get_run_logger()

    # Validate parameters
    if mode == ScraperMode.SPECIFIC_DATE and not specific_date:
        error_msg = "specific_date is required when mode is SPECIFIC_DATE"
        logger.error(error_msg)
        return ScrapeToBronzeResult(
            status=FlowStatus.ERROR,
            error=error_msg,
        )

    logger.info(
        "Starting scrape for %s using %s scraper (max %d pages, mode: %s)",
        city.value,
        scraper_type.value,
        max_pages,
        mode,
    )

    scrape_date = datetime.now(UTC)

    # Get scraper class and instantiate it
    logger.info("Initializing %s scraper", scraper_type.value)
    scraper_class = scraper_type.get_scraper_class()

    with scraper_class(
        user_agent=settings.scraper.user_agent,
        download_delay=settings.scraper.download_delay,
        scraper_mode=mode,
        days=days,
        specific_date=specific_date,
    ) as scraper:
        # Validate if city is supported by scraper
        if city not in scraper.supported_cities:
            error_msg = f"City not supported by {scraper_type.value}"
            logger.error(error_msg)
            return ScrapeToBronzeResult(
                status=FlowStatus.ERROR,
                error=error_msg,
            )

        # Fetch pages
        # TODO: Implement parallel page fetching using map_over in Prefect flow
        logger.info("Fetching %d pages for %s", max_pages, city.value)

        page_results = []
        for page in range(1, max_pages + 1):
            try:
                result = fetch_and_parse_page(scraper=scraper, city=city, page=page)
                page_results.append(result)
            except Exception:
                logger.error("Page %d failed after all retries", page, exc_info=True)

    # Aggregate results from all pages
    logger.info("Aggregating results from all pages")
    all_listings, failed_listings = aggregate_results(page_results)
    total_listings = len(all_listings)
    logger.info("Total: %d listings scraped, %d failed", total_listings, failed_listings)

    if total_listings == 0:
        logger.warning("No listings found to save")
        return ScrapeToBronzeResult(
            status=FlowStatus.COMPLETED_NO_DATA,
            total_listings=0,
            failed_listings=failed_listings,
        )

    # Save to MinIO (raw bucket)
    logger.info("Saving listings to MinIO")

    try:
        storage = MinIOStorage(
            endpoint=settings.minio.endpoint,
            access_key=settings.minio.access_key,
            secret_key=settings.minio.secret_key.get_secret_value(),
            bucket_name=settings.minio.bucket_name,
            secure=(settings.environment == Environment.PROD),
        )
        storage_result = save_listings_to_minio(
            listings=all_listings,
            storage=storage,
            source=scraper_type.value,
            city=city.value,
            partition_date=format_date(scrape_date),
            max_pages=max_pages,
            mode=mode,
            days=days,
            specific_date=specific_date,
        )
    except Exception:
        logger.error("Failed to save listings to MinIO", exc_info=True)
        return ScrapeToBronzeResult(
            status=FlowStatus.ERROR,
            total_listings=total_listings,
            failed_listings=failed_listings,
            error="Failed to save listings to MinIO",
        )

    logger.info("Scraping flow completed successfully")

    return ScrapeToBronzeResult(
        status=FlowStatus.SUCCESS,
        total_listings=total_listings,
        failed_listings=failed_listings,
        storage=storage_result,
    )


if __name__ == "__main__":
    # Example usage with Kijiji scraper for a specific date
    result = scrape_to_bronze(
        scraper_type=ScraperType.KIJIJI,
        city=City.TORONTO,
        max_pages=1,
        mode=ScraperMode.SPECIFIC_DATE,
        specific_date=date(2026, 2, 27),
    )
    # result = scrape_to_bronze(scraper_type=ScraperType.KIJIJI, city=City.TORONTO, max_pages=1, mode=ScraperMode.LAST_X_DAYS, days=1)
    print(f"Scraping result: {result}")
