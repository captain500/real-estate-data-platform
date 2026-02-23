"""Prefect flow for scraping rental listings."""

import datetime

from prefect import flow, get_run_logger

from real_estate_data_platform.config.settings import settings
from real_estate_data_platform.models.listings import City, FlowResult, FlowStatus
from real_estate_data_platform.scrapers.base_scraper import BaseScraper
from real_estate_data_platform.scrapers.scraper_type import ScraperType
from real_estate_data_platform.tasks.load_bronze import save_listings_to_minio
from real_estate_data_platform.tasks.scraping import (
    aggregate_results,
    fetch_and_parse_page,
)


@flow(name="scrape-to-bronze")
def scrape_to_bronze(
    scraper_type: ScraperType, city: City = City.TORONTO, max_pages: int = 5
) -> FlowResult:
    """Scrape listings from a rental website and save to MinIO raw bucket.

    This flow:
    1. Instantiates the specified scraper type
    2. Fetches multiple pages in parallel
    3. Aggregates results
    4. Saves to MinIO in partitioned Parquet format

    Args:
        scraper_type: ScraperType enum specifying which scraper to use
        city: City to scrape (City enum)
        max_pages: Maximum number of pages to scrape

    Returns:
        FlowResult with scraping metadata and results
    """
    flow_logger = get_run_logger()

    flow_logger.info(
        f"Starting scrape for {city.value} using {scraper_type.value} scraper "
        f"(max {max_pages} pages)"
    )

    scrape_date = datetime.datetime.now(datetime.UTC)

    # Get configuration from settings
    user_agent = settings.scraper.user_agent
    download_delay = settings.scraper.download_delay
    minio_endpoint = settings.minio.endpoint
    minio_access_key = settings.minio.access_key
    minio_secret_key = settings.minio.secret_key.get_secret_value()
    bucket_name = settings.minio.bucket_name

    # Get scraper class and instantiate it
    flow_logger.info(f"Initializing {scraper_type.value} scraper")
    scraper_class: type[BaseScraper] = scraper_type.get_scraper_class()
    scraper = scraper_class(user_agent=user_agent, download_delay=download_delay)

    # Validate if city is supported by scraper
    if city not in scraper.SUPPORTED_CITIES:
        flow_logger.error(
            f"City '{city.value}' not supported by {scraper_type.value}. "
            f"Supported cities: {[c.value for c in scraper.SUPPORTED_CITIES.keys()]}"
        )
        return FlowResult(
            status=FlowStatus.ERROR,
            scraper_type=scraper_type.value,
            city=city.value,
            error=f"City not supported by {scraper_type.value}",
        )

    # TODO Fetch pages in parallel
    flow_logger.info(f"Fetching {max_pages} pages for {city.value}")

    page_results = []
    for page in range(1, max_pages + 1):
        result = fetch_and_parse_page(
            scraper=scraper, city=city, page=page, download_delay=download_delay
        )
        page_results.append(result)

    # Aggregate results from all pages
    flow_logger.info("Aggregating results from all pages")
    all_listings = aggregate_results(page_results)

    total_listings = len(all_listings)
    flow_logger.info(f"Total listings aggregated: {total_listings}")

    if total_listings == 0:
        flow_logger.warning("No listings found to save")
        scraper.close()
        return FlowResult(
            status=FlowStatus.COMPLETED_NO_DATA,
            scraper_type=scraper_type.value,
            city=city.value,
            pages_scraped=max_pages,
            scrape_date=scrape_date,
        )

    # Save to MinIO (raw bucket)
    flow_logger.info("Saving listings to MinIO")

    storage_result = save_listings_to_minio(
        listings=all_listings,
        source=scraper_type.value,
        city=city.value,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        bucket_name=bucket_name,
        partition_date=scrape_date.strftime("%Y-%m-%d"),
    )

    scraper.close()
    flow_logger.info("Scraping flow completed successfully")

    return FlowResult(
        status=FlowStatus.SUCCESS,
        scraper_type=scraper_type.value,
        city=city.value,
        pages_scraped=max_pages,
        total_listings=total_listings,
        scrape_date=scrape_date,
        storage=storage_result,
    )


if __name__ == "__main__":
    # Example usage with Kijiji scraper
    result = scrape_to_bronze(scraper_type=ScraperType.KIJIJI, city=City.TORONTO, max_pages=1)
    print(f"Scraping result: {result}")
