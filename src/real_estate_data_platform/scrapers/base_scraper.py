"""Base scraper class defining the interface for all scrapers."""

import logging
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

from real_estate_data_platform.models.enums import City, ScraperMode
from real_estate_data_platform.models.listings import RentalsListing

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class for all web scrapers.

    Implements the Template Method pattern for consistent parsing and date filtering
    across all scraper implementations.
    """

    def __init__(
        self,
        user_agent: str,
        download_delay: float = 2.0,
        scraper_mode: ScraperMode = ScraperMode.LAST_X_DAYS,
        days: int = 7,
        specific_date: date | None = None,
    ):
        """Initialize scraper.

        Args:
            user_agent: User agent string for requests
            download_delay: Delay between requests in seconds
            scraper_mode: Mode for date filtering (last_x_days or specific_date)
            days: Number of days for last_x_days mode
            specific_date: Specific date for specific_date mode
        """
        self.user_agent = user_agent
        self.download_delay = download_delay
        self.scraper_mode = scraper_mode
        self.days = days
        self.specific_date = specific_date
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    @property
    @abstractmethod
    def NAME_WEBSITE(self) -> str:
        """Name of the scraper (e.g: 'kijiji')."""
        pass

    @property
    @abstractmethod
    def BASE_URL(self) -> str:
        """URL of the website (e.g: 'https://www.kijiji.ca')."""
        pass

    @property
    @abstractmethod
    def SUPPORTED_CITIES(self) -> dict[City, str]:
        """Mapping of City enum values to web-specific slugs/IDs."""
        pass

    @abstractmethod
    def get_page(self, city: City, page: int = 1) -> BeautifulSoup:
        """Fetch and parse a page.

        Args:
            city: City to search (City enum)
            page: Page number (1-based)

        Returns:
            BeautifulSoup object of the page

        Raises:
            requests.HTTPError: If HTTP request fails
            ValueError: If city not supported
        """
        pass

    @abstractmethod
    def parse_listing(self, listing_elem, city: City) -> RentalsListing | None:
        """Parse a single listing element.

        Args:
            listing_elem: BeautifulSoup element containing listing HTML
            city: City being scraped

        Returns:
            RentalsListing object or None if parsing fails
        """
        pass

    @abstractmethod
    def _parse_page_impl(
        self,
        soup: BeautifulSoup,
        city: City,
    ) -> list[RentalsListing]:
        """Internal implementation for parsing listings from a page.

        Subclasses should implement this method with their specific parsing logic.
        Filtering by date is handled automatically by parse_page().

        Args:
            soup: BeautifulSoup object of the page
            city: City being scraped (City enum - for labeling)

        Returns:
            List of parsed RentalsListing objects (before date filtering)
        """
        pass

    def parse_page(
        self,
        soup: BeautifulSoup,
        city: City,
    ) -> list[RentalsListing]:
        """Template method that parses listings and applies date filtering.

        This method orchestrates the parsing process and automatically applies
        date filtering based on scraper mode settings.

        Args:
            soup: BeautifulSoup object of the page
            city: City being scraped (City enum - for labeling)

        Returns:
            List of parsed RentalsListing objects (already date-filtered)
        """
        raw_listings = self._parse_page_impl(soup, city)
        filtered_listings = self._apply_date_filter(raw_listings, city.value)
        return filtered_listings

    def _passes_date_filter(self, listing: RentalsListing) -> bool:
        """Check if a listing passes the date filter based on scraper mode.

        Args:
            listing: RentalsListing object to check

        Returns:
            True if listing should be included, False otherwise
        """
        if self.scraper_mode == ScraperMode.LAST_X_DAYS:
            cutoff_date = datetime.now(UTC) - timedelta(days=self.days)
            return listing.published_at >= cutoff_date
        elif self.scraper_mode == ScraperMode.SPECIFIC_DATE and self.specific_date:
            start_of_day = datetime.combine(self.specific_date, datetime.min.time()).replace(
                tzinfo=UTC
            )
            end_of_day = datetime.combine(self.specific_date, datetime.max.time()).replace(
                tzinfo=UTC
            )
            return start_of_day <= listing.published_at <= end_of_day
        return True

    def _apply_date_filter(self, listings: list[RentalsListing], city: str) -> list[RentalsListing]:
        """Apply date filter to listings and log results.

        Args:
            listings: List of listings to filter
            city: City name for logging

        Returns:
            Filtered list of listings
        """
        # TODO: Optimize date filtering to stop scraping once a listing date
        # is found to be older than the target date.
        # Note: Featured listings appear first and may have significantly different
        # dates, so early exit logic needs to account for this behavior.
        filtered_listings = [listing for listing in listings if self._passes_date_filter(listing)]
        filtered_count = len(listings) - len(filtered_listings)

        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} listings by date for {city}")

        return filtered_listings

    def close(self):
        """Close any resources (sessions, connections, etc.)."""
        self.session.close()
