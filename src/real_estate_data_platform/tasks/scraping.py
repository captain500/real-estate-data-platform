"""Prefect tasks for web scraping operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from prefect import get_run_logger, task

from real_estate_data_platform.models.enums import City
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import ScrapingResult

if TYPE_CHECKING:
    from real_estate_data_platform.scrapers.base_scraper import BaseScraper


@task(
    retries=3,
    retry_delay_seconds=2,
)
def fetch_and_parse_page(
    scraper: BaseScraper,
    city: City,
    page: int,
) -> ScrapingResult:
    """Fetch and parse a single page using the provided scraper.

    Args:
        scraper: Instance of a BaseScraper subclass (e.g., KijijiScraper)
        city: City to scrape (City enum)
        page: Page number

    Returns:
        ScrapingResult containing parsed listings
    """
    logger = get_run_logger()

    logger.info("Fetching page %d for %s using %s", page, city.value, scraper.__class__.__name__)

    soup = scraper.get_page(city=city, page=page)

    listings, failed_listings = scraper.parse_page(soup=soup, city=city)

    logger.info("Parsed %d listings from page %d (%d failed)", len(listings), page, failed_listings)

    return ScrapingResult(
        page_number=page,
        listings=listings,
        failed_listings=failed_listings,
    )


@task
def aggregate_results(results: list[ScrapingResult]) -> tuple[list[RentalsListing], int]:
    """Flatten listings from multiple page results into a single list.

    Args:
        results: List of successful ScrapingResult objects

    Returns:
        Tuple of (all listings, total failed listings)
    """
    logger = get_run_logger()

    all_listings = [listing for result in results for listing in result.listings]
    total_failed = sum(result.failed_listings for result in results)

    logger.info("Aggregated %d listings (%d failed)", len(all_listings), total_failed)

    return all_listings, total_failed
