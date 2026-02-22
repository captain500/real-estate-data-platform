"""Base scraper class defining the interface for all scrapers."""

from abc import ABC, abstractmethod

import requests
from bs4 import BeautifulSoup

from real_estate_data_platform.models.listings import City, RentalsListing


class BaseScraper(ABC):
    """Abstract base class for all web scrapers."""

    def __init__(self, user_agent: str, download_delay: float = 2.0):
        """Initialize scraper.

        Args:
            user_agent: User agent string for requests
            download_delay: Delay between requests in seconds
        """
        self.user_agent = user_agent
        self.download_delay = download_delay
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
    def parse_listing(self, listing_elem) -> RentalsListing | None:
        """Parse a single listing element.

        Args:
            listing_elem: BeautifulSoup element containing listing HTML

        Returns:
            RentalsListing object or None if parsing fails
        """
        pass

    @abstractmethod
    def parse_page(self, soup: BeautifulSoup, city: City) -> list[RentalsListing]:
        """Parse all listings from a page.

        Args:
            soup: BeautifulSoup object of the page
            city: City being scraped (City enum - for labeling)

        Returns:
            List of parsed RentalsListing objects
        """
        pass

    def close(self):
        """Close any resources (sessions, connections, etc.)."""
        self.session.close()
