"""Enum for scraper types."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from real_estate_data_platform.scrapers.base_scraper import BaseScraper


class ScraperType(StrEnum):
    """Available scraper types."""

    KIJIJI = "kijiji"

    def get_scraper_class(self) -> type[BaseScraper]:
        """Get scraper class for this scraper type."""
        from real_estate_data_platform.scrapers.kijiji_scraper import KijijiScraper

        scraper_map: dict[ScraperType, type[BaseScraper]] = {
            ScraperType.KIJIJI: KijijiScraper,
        }
        return scraper_map[self]
