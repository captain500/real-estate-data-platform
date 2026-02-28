"""Shared fixtures for scraper tests."""

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from real_estate_data_platform.scrapers.kijiji_scraper import KijijiScraper

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(filename: str) -> str:
    """Load an HTML fixture file as a string."""
    return (FIXTURES_DIR / filename).read_text(encoding="utf-8")


def load_fixture_soup(filename: str) -> BeautifulSoup:
    """Load an HTML fixture file and return a BeautifulSoup object."""
    return BeautifulSoup(load_fixture(filename), "html.parser")


@pytest.fixture
def kijiji_scraper():
    """Create a KijijiScraper instance with no download delay."""
    scraper = KijijiScraper(
        user_agent="TestAgent/1.0",
        download_delay=0,
    )
    yield scraper
    scraper.close()


@pytest.fixture
def search_page_soup():
    """Load the search results page fixture."""
    return load_fixture_soup("kijiji_search_page.html")


@pytest.fixture
def search_empty_soup():
    """Load the empty search results page fixture."""
    return load_fixture_soup("kijiji_search_empty.html")


@pytest.fixture
def listing_detail_html():
    """Load the listing detail page HTML as a string."""
    return load_fixture("kijiji_listing_detail.html")


@pytest.fixture
def listing_detail_soup():
    """Load the listing detail page fixture."""
    return load_fixture_soup("kijiji_listing_detail.html")


@pytest.fixture
def listing_no_neighbourhood_html():
    """Load the listing without neighbourhood fixture as a string."""
    return load_fixture("kijiji_listing_no_neighbourhood.html")


@pytest.fixture
def listing_no_neighbourhood_soup():
    """Load the listing without neighbourhood fixture."""
    return load_fixture_soup("kijiji_listing_no_neighbourhood.html")


@pytest.fixture
def listing_no_next_data_html():
    """Load the listing without __NEXT_DATA__ fixture as a string."""
    return load_fixture("kijiji_listing_no_next_data.html")
