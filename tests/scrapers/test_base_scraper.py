"""Unit tests for BaseScraper date filtering logic."""

from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from real_estate_data_platform.models.enums import City, ScraperMode
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.scrapers.kijiji_scraper import KijijiScraper


def _make_listing(published_at: datetime, **kwargs) -> RentalsListing:
    """Create a minimal RentalsListing for testing date filters."""
    defaults = {
        "listing_id": "test-001",
        "url": "https://www.kijiji.ca/v-apartments-condos/toronto/test/1",
        "website": "kijiji",
        "published_at": published_at,
        "title": "Test listing",
        "description": "A test listing for unit tests",
        "street": "123 Test St",
        "city": City.TORONTO,
    }
    defaults.update(kwargs)
    return RentalsListing(**defaults)


# ---------------------------------------------------------------------------
# _passes_date_filter
# ---------------------------------------------------------------------------
class TestPassesDateFilter:
    """Tests for the date filter predicate."""

    def test_last_x_days_includes_recent_listing(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=7,
        )
        listing = _make_listing(published_at=datetime.now(UTC) - timedelta(days=3))
        assert scraper._passes_date_filter(listing) is True

    def test_last_x_days_excludes_old_listing(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=7,
        )
        listing = _make_listing(published_at=datetime.now(UTC) - timedelta(days=10))
        assert scraper._passes_date_filter(listing) is False

    def test_last_x_days_boundary_exactly_at_cutoff(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=7,
        )
        # Listing exactly at the cutoff edge (should pass since >= comparison)
        listing = _make_listing(published_at=datetime.now(UTC) - timedelta(days=7, seconds=-1))
        assert scraper._passes_date_filter(listing) is True

    def test_specific_date_includes_matching_date(self):
        target_date = date(2026, 2, 25)
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.SPECIFIC_DATE,
            specific_date=target_date,
        )
        listing = _make_listing(published_at=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC))
        assert scraper._passes_date_filter(listing) is True

    def test_specific_date_excludes_different_date(self):
        target_date = date(2026, 2, 25)
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.SPECIFIC_DATE,
            specific_date=target_date,
        )
        listing = _make_listing(published_at=datetime(2026, 2, 24, 23, 59, 59, tzinfo=UTC))
        assert scraper._passes_date_filter(listing) is False

    def test_specific_date_includes_start_of_day(self):
        target_date = date(2026, 2, 25)
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.SPECIFIC_DATE,
            specific_date=target_date,
        )
        listing = _make_listing(published_at=datetime(2026, 2, 25, 0, 0, 0, tzinfo=UTC))
        assert scraper._passes_date_filter(listing) is True

    def test_specific_date_includes_end_of_day(self):
        target_date = date(2026, 2, 25)
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.SPECIFIC_DATE,
            specific_date=target_date,
        )
        listing = _make_listing(published_at=datetime(2026, 2, 25, 23, 59, 59, 999999, tzinfo=UTC))
        assert scraper._passes_date_filter(listing) is True

    def test_specific_date_mode_without_date_passes_all(self):
        """SPECIFIC_DATE mode but specific_date is None → passes everything."""
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.SPECIFIC_DATE,
            specific_date=None,
        )
        listing = _make_listing(published_at=datetime(2020, 1, 1, tzinfo=UTC))
        assert scraper._passes_date_filter(listing) is True


# ---------------------------------------------------------------------------
# _apply_date_filter
# ---------------------------------------------------------------------------
class TestApplyDateFilter:
    """Tests for bulk date filtering with logging."""

    def test_filters_out_old_listings(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=7,
        )
        recent = _make_listing(
            listing_id="recent",
            published_at=datetime.now(UTC) - timedelta(days=2),
        )
        old = _make_listing(
            listing_id="old",
            published_at=datetime.now(UTC) - timedelta(days=30),
        )

        result = scraper._apply_date_filter([recent, old], "toronto")
        assert len(result) == 1
        assert result[0].listing_id == "recent"

    def test_returns_all_when_all_pass(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=30,
        )
        listings = [
            _make_listing(listing_id=f"l{i}", published_at=datetime.now(UTC) - timedelta(days=i))
            for i in range(5)
        ]

        result = scraper._apply_date_filter(listings, "toronto")
        assert len(result) == 5

    def test_returns_empty_when_none_pass(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=1,
        )
        listings = [
            _make_listing(
                listing_id=f"l{i}",
                published_at=datetime.now(UTC) - timedelta(days=10 + i),
            )
            for i in range(3)
        ]

        result = scraper._apply_date_filter(listings, "toronto")
        assert result == []

    def test_empty_input_returns_empty(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
        )
        assert scraper._apply_date_filter([], "toronto") == []


# ---------------------------------------------------------------------------
# parse_page (template method)
# ---------------------------------------------------------------------------
class TestParsePage:
    """Tests for the parse_page template method."""

    def test_combines_parsing_and_date_filtering(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=7,
        )

        recent = _make_listing(
            listing_id="recent",
            published_at=datetime.now(UTC) - timedelta(days=1),
        )
        old = _make_listing(
            listing_id="old",
            published_at=datetime.now(UTC) - timedelta(days=30),
        )

        with (
            MagicMock() as mock_soup,
            pytest.MonkeyPatch.context() as mp,
        ):
            mp.setattr(scraper, "_parse_page_impl", lambda soup, city: ([recent, old], 1))

            listings, failed = scraper.parse_page(mock_soup, City.TORONTO)

        assert len(listings) == 1
        assert listings[0].listing_id == "recent"
        assert failed == 1

    def test_no_listings_after_filter(self):
        scraper = KijijiScraper(
            user_agent="TestAgent/1.0",
            download_delay=0,
            scraper_mode=ScraperMode.LAST_X_DAYS,
            days=1,
        )

        old = _make_listing(
            listing_id="old",
            published_at=datetime.now(UTC) - timedelta(days=30),
        )

        with (
            MagicMock() as mock_soup,
            pytest.MonkeyPatch.context() as mp,
        ):
            mp.setattr(scraper, "_parse_page_impl", lambda soup, city: ([old], 0))
            listings, failed = scraper.parse_page(mock_soup, City.TORONTO)

        assert listings == []
        assert failed == 0


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------
class TestScraperInit:
    """Tests for scraper construction."""

    def test_default_values(self):
        scraper = KijijiScraper(user_agent="TestAgent/1.0")
        assert scraper.user_agent == "TestAgent/1.0"
        assert scraper.download_delay == 2.0
        assert scraper.scraper_mode == ScraperMode.LAST_X_DAYS
        assert scraper.days == 7
        assert scraper.specific_date is None
        scraper.close()

    def test_custom_values(self):
        scraper = KijijiScraper(
            user_agent="CustomAgent/2.0",
            download_delay=5.0,
            scraper_mode=ScraperMode.SPECIFIC_DATE,
            days=14,
            specific_date=date(2026, 2, 25),
        )
        assert scraper.user_agent == "CustomAgent/2.0"
        assert scraper.download_delay == 5.0
        assert scraper.scraper_mode == ScraperMode.SPECIFIC_DATE
        assert scraper.days == 14
        assert scraper.specific_date == date(2026, 2, 25)
        scraper.close()

    def test_session_has_user_agent(self):
        scraper = KijijiScraper(user_agent="TestAgent/1.0", download_delay=0)
        assert scraper.session.headers["User-Agent"] == "TestAgent/1.0"
        scraper.close()
