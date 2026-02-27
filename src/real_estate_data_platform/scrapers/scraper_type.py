"""Enum for scraper types."""

from enum import StrEnum


class ScraperType(StrEnum):
    """Available scraper types."""

    KIJIJI = "kijiji"

    def get_scraper_class(self):
        """Get scraper class for this scraper type.

        Returns:
            Scraper class

        Raises:
            ValueError: If scraper type not supported
        """
        from real_estate_data_platform.scrapers.kijiji_scraper import KijijiScraper

        scraper_map = {
            ScraperType.KIJIJI: KijijiScraper,
        }

        if self not in scraper_map:
            raise ValueError(f"Unsupported scraper type: {self}")

        return scraper_map[self]
