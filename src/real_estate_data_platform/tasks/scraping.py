"""Prefect tasks for web scraping operations."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from prefect import get_run_logger, task
from prefect.tasks import task_input_hash

from real_estate_data_platform.models.listings import (
    City,
    RentalsListing,
    ScrapingResult,
)

if TYPE_CHECKING:
    from real_estate_data_platform.scrapers.base_scraper import BaseScraper


@task(
    retries=3,
    retry_delay_seconds=2,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def fetch_and_parse_page(
    scraper: BaseScraper,
    city: City,
    page: int,
    download_delay: float = 2.0,
) -> ScrapingResult:
    """Fetch and parse a single page using the provided scraper.

    Args:
        scraper: Instance of a BaseScraper subclass (e.g., KijijiScraper)
        city: City to scrape (City enum)
        page: Page number
        download_delay: Delay between requests

    Returns:
        ScrapingResult containing parsed listings
    """
    task_logger = get_run_logger()

    try:
        task_logger.info(
            f"Fetching page {page} for {city.value} using {scraper.__class__.__name__}"
        )

        # Fetch page
        soup = scraper.get_page(city=city, page=page)

        # Parse listings of house for rent
        listings = scraper.parse_page(soup=soup, city=city, download_delay=download_delay)

        task_logger.info(f"Parsed {len(listings)} listings from page {page}")

        result = ScrapingResult(
            page_number=page,
            city=city.value,
            listings=listings,
            total_listings=len(listings),
        )

        return result

    except Exception as e:
        task_logger.error(f"Error scraping page {page}: {e}")
        return ScrapingResult(
            page_number=page,
            city=city.value,
            total_listings=0,
            listings=[],
            error=str(e),
        )


@task
def aggregate_results(results: list[ScrapingResult]) -> list[RentalsListing]:
    """Aggregate results from multiple pages into a single list.

    Args:
        results: List of ScrapingResult objects from different pages

    Returns:
        Combined list of all RentalsListing objects
    """
    task_logger = get_run_logger()

    all_listings = []
    error_count = 0

    for result in results:
        if result.error:
            task_logger.warning(f"Page {result.page_number} had error: {result.error}")
            error_count += 1
        all_listings.extend(result.listings)

    task_logger.info(f"Aggregated {len(all_listings)} listings ({error_count} pages with errors)")

    return all_listings
