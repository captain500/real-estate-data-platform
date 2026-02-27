"""Prefect flow for scraping rental listings."""

from datetime import UTC, date, datetime

from prefect import flow, get_run_logger

from real_estate_data_platform.config.settings import settings
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
    flow_logger = get_run_logger()

    # Validate parameters
    if mode == ScraperMode.SPECIFIC_DATE and not specific_date:
        error_msg = "specific_date is required when mode is SPECIFIC_DATE"
        flow_logger.error(error_msg)
        return ScrapeToBronzeResult(
            status=FlowStatus.ERROR,
            error=error_msg,
        )

    flow_logger.info(
        f"Starting scrape for {city.value} using {scraper_type.value} scraper "
        f"(max {max_pages} pages, mode: {mode})"
    )

    scrape_date = datetime.now(UTC)

    # Get scraper class and instantiate it
    flow_logger.info(f"Initializing {scraper_type.value} scraper")
    scraper_class = scraper_type.get_scraper_class()
    scraper = scraper_class(
        user_agent=settings.scraper.user_agent,
        download_delay=settings.scraper.download_delay,
        scraper_mode=mode,
        days=days,
        specific_date=specific_date,
    )

    # Validate if city is supported by scraper
    if city not in scraper.SUPPORTED_CITIES:
        error_msg = f"City not supported by {scraper_type.value}"
        flow_logger.error(error_msg)
        return ScrapeToBronzeResult(
            status=FlowStatus.ERROR,
            error=error_msg,
        )

    # Fetch pages
    # TODO: Implement parallel page fetching using map_over in Prefect flow
    flow_logger.info(f"Fetching {max_pages} pages for {city.value}")

    page_results = []
    for page in range(1, max_pages + 1):
        result = fetch_and_parse_page(scraper=scraper, city=city, page=page)
        page_results.append(result)

    # Aggregate results from all pages
    flow_logger.info("Aggregating results from all pages")
    all_listings, failed_pages = aggregate_results(page_results)
    total_listings = len(all_listings)
    flow_logger.info(f"Total listings aggregated: {total_listings}")

    if total_listings == 0:
        flow_logger.warning("No listings found to save")
        scraper.close()
        return ScrapeToBronzeResult(
            status=FlowStatus.COMPLETED_NO_DATA,
        )

    # Save to MinIO (raw bucket)
    flow_logger.info("Saving listings to MinIO")

    storage_result = save_listings_to_minio(
        listings=all_listings,
        source=scraper_type.value,
        city=city.value,
        minio_endpoint=settings.minio.endpoint,
        minio_access_key=settings.minio.access_key,
        minio_secret_key=settings.minio.secret_key.get_secret_value(),
        partition_date=format_date(scrape_date),
        max_pages=max_pages,
        mode=mode,
        days=days,
        specific_date=specific_date,
        environment=settings.environment.value,
        bucket_name=settings.minio.bucket_name,
    )

    scraper.close()
    flow_logger.info("Scraping flow completed successfully")

    return ScrapeToBronzeResult(
        status=FlowStatus.SUCCESS,
        successful_pages=total_listings,
        failed_pages=failed_pages,
        storage=storage_result,
    )


if __name__ == "__main__":
    # Example usage with Kijiji scraper for a specific date
    result = scrape_to_bronze(
        scraper_type=ScraperType.KIJIJI,
        city=City.TORONTO,
        max_pages=1,
        mode=ScraperMode.SPECIFIC_DATE,
        specific_date=date(2026, 2, 25),
    )
    # result = scrape_to_bronze(scraper_type=ScraperType.KIJIJI, city=City.TORONTO, max_pages=1, mode=ScraperMode.LAST_X_DAYS, days=1)
    print(f"Scraping result: {result}")
