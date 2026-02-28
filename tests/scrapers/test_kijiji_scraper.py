"""Unit tests for KijijiScraper."""

import json
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from real_estate_data_platform.models.enums import City
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.scrapers.kijiji_scraper import ATTRIBUTE_MAPPING, KijijiScraper


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------
class TestKijijiScraperProperties:
    """Tests for scraper property accessors."""

    def test_name_website(self, kijiji_scraper):
        assert kijiji_scraper.name_website == "kijiji"

    def test_base_url(self, kijiji_scraper):
        assert kijiji_scraper.base_url == "https://www.kijiji.ca/b-apartments-condos"

    def test_supported_cities_contains_all_expected(self, kijiji_scraper):
        supported = kijiji_scraper.supported_cities
        assert City.TORONTO in supported
        assert City.VANCOUVER in supported
        assert City.LONDON in supported

    def test_supported_cities_values_are_url_paths(self, kijiji_scraper):
        for path in kijiji_scraper.supported_cities.values():
            assert "/" in path  # e.g. "city-of-toronto/c37l1700273"


# ---------------------------------------------------------------------------
# _extract_json_ld
# ---------------------------------------------------------------------------
class TestExtractJsonLd:
    """Tests for JSON-LD extraction from HTML."""

    def test_returns_parsed_dict_from_search_page(self, kijiji_scraper, search_page_soup):
        data = kijiji_scraper._extract_json_ld(search_page_soup)
        assert data is not None
        assert data["@type"] == "ItemList"
        assert len(data["itemListElement"]) == 3

    def test_returns_none_when_no_json_ld(self, kijiji_scraper, search_empty_soup):
        data = kijiji_scraper._extract_json_ld(search_empty_soup)
        assert data is None

    def test_returns_none_for_invalid_json(self, kijiji_scraper):
        html = (
            '<html><body><script type="application/ld+json">NOT VALID JSON</script></body></html>'
        )
        soup = BeautifulSoup(html, "html.parser")
        data = kijiji_scraper._extract_json_ld(soup)
        assert data is None

    def test_returns_none_for_empty_script(self, kijiji_scraper):
        html = '<html><body><script type="application/ld+json"></script></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        data = kijiji_scraper._extract_json_ld(soup)
        assert data is None


# ---------------------------------------------------------------------------
# _extract_attributes
# ---------------------------------------------------------------------------
class TestExtractAttributes:
    """Tests for attribute extraction and normalisation."""

    def test_maps_known_attributes(self, kijiji_scraper):
        listing_data = {
            "attributes": {
                "all": [
                    {"name": "Bedrooms", "values": ["2"]},
                    {"name": "Bathrooms", "values": ["1"]},
                    {"name": "Size (sqft)", "values": ["850"]},
                    {"name": "Pet Friendly", "values": ["Yes"]},
                    {"name": "Air Conditioning", "values": ["Yes"]},
                ]
            }
        }
        attrs = kijiji_scraper._extract_attributes(listing_data)

        assert attrs["bedrooms"] == "2"
        assert attrs["bathrooms"] == "1"
        assert attrs["size_sqft"] == "850"
        assert attrs["pet_friendly"] == "Yes"
        assert attrs["air_conditioning"] == "Yes"

    def test_ignores_unknown_attributes(self, kijiji_scraper):
        listing_data = {
            "attributes": {
                "all": [
                    {"name": "Unknown Field", "values": ["whatever"]},
                    {"name": "Bedrooms", "values": ["3"]},
                ]
            }
        }
        attrs = kijiji_scraper._extract_attributes(listing_data)
        assert "Unknown Field" not in attrs
        assert attrs["bedrooms"] == "3"

    def test_handles_empty_attributes(self, kijiji_scraper):
        listing_data = {"attributes": {"all": []}}
        attrs = kijiji_scraper._extract_attributes(listing_data)
        assert attrs == {}

    def test_handles_missing_attributes_key(self, kijiji_scraper):
        listing_data = {}
        attrs = kijiji_scraper._extract_attributes(listing_data)
        assert attrs == {}

    def test_handles_attribute_with_none_value(self, kijiji_scraper):
        listing_data = {
            "attributes": {
                "all": [
                    {"name": "Bedrooms", "values": [None]},
                ]
            }
        }
        attrs = kijiji_scraper._extract_attributes(listing_data)
        assert attrs["bedrooms"] is None

    def test_all_attribute_mapping_keys_are_strings(self):
        """Ensure ATTRIBUTE_MAPPING is well-formed."""
        for key, value in ATTRIBUTE_MAPPING.items():
            assert isinstance(key, str)
            assert isinstance(value, str)


# ---------------------------------------------------------------------------
# _extract_neighbourhood_info
# ---------------------------------------------------------------------------
class TestExtractNeighbourhoodInfo:
    """Tests for neighbourhood and transportation scores extraction."""

    def test_extracts_neighbourhood_and_scores(self, kijiji_scraper, listing_detail_soup):
        script = listing_detail_soup.find("script", id="__NEXT_DATA__")
        data = json.loads(script.string)
        page_props = data["props"]["pageProps"]
        apollo_state = page_props["__APOLLO_STATE__"]
        listing_data = apollo_state["RealEstateListing:123456789"]

        neighbourhood, scores = kijiji_scraper._extract_neighbourhood_info(
            listing_data, apollo_state
        )

        assert neighbourhood == "Downtown Toronto"
        assert scores["walk_score"] == 9.2
        assert scores["transit_score"] == 8.5
        assert scores["bike_score"] == 7.8

    def test_returns_none_when_no_neighbourhood_ref(self, kijiji_scraper):
        listing_data = {"location": {"neighbourhoodInfo": {}}}
        apollo_state = {}

        neighbourhood, scores = kijiji_scraper._extract_neighbourhood_info(
            listing_data, apollo_state
        )

        assert neighbourhood is None
        assert scores == {"walk_score": None, "transit_score": None, "bike_score": None}

    def test_returns_none_when_ref_not_in_apollo(self, kijiji_scraper):
        listing_data = {"location": {"neighbourhoodInfo": {"__ref": "Neighbourhood:nonexistent"}}}
        apollo_state = {}

        neighbourhood, scores = kijiji_scraper._extract_neighbourhood_info(
            listing_data, apollo_state
        )

        assert neighbourhood is None
        assert scores == {"walk_score": None, "transit_score": None, "bike_score": None}

    def test_returns_name_without_scores(self, kijiji_scraper):
        listing_data = {"location": {"neighbourhoodInfo": {"__ref": "Neighbourhood:midtown"}}}
        apollo_state = {
            "Neighbourhood:midtown": {
                "name": "Midtown",
                "scores": {},
            }
        }

        neighbourhood, scores = kijiji_scraper._extract_neighbourhood_info(
            listing_data, apollo_state
        )

        assert neighbourhood == "Midtown"
        assert scores == {"walk_score": None, "transit_score": None, "bike_score": None}


# ---------------------------------------------------------------------------
# _parse_listing_detail  (mocking HTTP, using HTML fixtures)
# ---------------------------------------------------------------------------
class TestParseListingDetail:
    """Tests for individual listing detail page parsing."""

    def test_parses_full_listing(self, kijiji_scraper, listing_detail_html):
        mock_response = MagicMock()
        mock_response.text = listing_detail_html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/beautiful-2br-apartment/123456789",
                City.TORONTO,
            )

        assert listing is not None
        assert isinstance(listing, RentalsListing)
        assert listing.listing_id == "123456789"
        assert listing.title == "Beautiful 2BR Apartment Downtown"
        assert listing.street == "123 Queen Street West"
        assert listing.city == City.TORONTO
        assert listing.neighbourhood == "Downtown Toronto"
        assert listing.rent == 2500.0  # 250000 / 100
        assert listing.bedrooms == 2
        assert listing.bathrooms == 1
        assert listing.size_sqft == 850.0
        assert listing.latitude == 43.6532
        assert listing.longitude == -79.3832
        assert listing.unit_type == "Apartment"
        assert listing.pet_friendly == "Yes"
        assert listing.air_conditioning == "Yes"
        assert listing.balcony == "Yes"
        assert listing.walk_score == 9.2
        assert listing.transit_score == 8.5
        assert listing.bike_score == 7.8
        assert len(listing.images) == 2

    def test_parses_listing_without_neighbourhood(
        self, kijiji_scraper, listing_no_neighbourhood_html
    ):
        mock_response = MagicMock()
        mock_response.text = listing_no_neighbourhood_html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/studio/555666777",
                City.TORONTO,
            )

        assert listing is not None
        assert listing.listing_id == "555666777"
        assert listing.neighbourhood is None
        assert listing.walk_score is None
        assert listing.transit_score is None
        assert listing.bike_score is None
        assert listing.rent == 1200.0  # 120000 / 100
        assert listing.bedrooms == 0
        assert listing.furnished == "Yes"

    def test_returns_none_when_no_next_data(self, kijiji_scraper, listing_no_next_data_html):
        mock_response = MagicMock()
        mock_response.text = listing_no_next_data_html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/some-listing/999",
                City.TORONTO,
            )

        assert listing is None

    def test_returns_none_on_http_error(self, kijiji_scraper):
        import requests

        with patch.object(
            kijiji_scraper.session,
            "get",
            side_effect=requests.RequestException("Connection failed"),
        ):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/bad/000",
                City.TORONTO,
            )

        assert listing is None

    def test_returns_none_when_listing_id_missing(self, kijiji_scraper):
        """__NEXT_DATA__ exists but has no listingId."""
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props": {"pageProps": {}}}
        </script>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/no-id/000",
                City.TORONTO,
            )

        assert listing is None

    def test_returns_none_when_listing_data_empty(self, kijiji_scraper):
        """__NEXT_DATA__ has listingId but no corresponding apollo data."""
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props": {"pageProps": {"listingId": "999", "__APOLLO_STATE__": {}}}}
        </script>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/empty/999",
                City.TORONTO,
            )

        assert listing is None

    def test_price_conversion_small_value_not_divided(self, kijiji_scraper):
        """Price <= 100 should NOT be divided by 100."""
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {
            "props": {
                "pageProps": {
                    "listingId": "777",
                    "activationDate": "2026-02-20T10:00:00.000Z",
                    "__APOLLO_STATE__": {
                        "RealEstateListing:777": {
                            "title": "Test",
                            "description": "Test description",
                            "price": {"amount": 50},
                            "activationDate": "2026-02-20T10:00:00.000Z",
                            "location": {
                                "address": "1 Test Rd",
                                "coordinates": {"latitude": 43.0, "longitude": -79.0},
                                "neighbourhoodInfo": {}
                            },
                            "imageUrls": [],
                            "attributes": {"all": []}
                        }
                    }
                }
            }
        }
        </script>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper._parse_listing_detail(
                "https://www.kijiji.ca/v-apartments-condos/toronto/test/777",
                City.TORONTO,
            )

        assert listing is not None
        assert listing.rent == 50  # Not divided because <= 100


# ---------------------------------------------------------------------------
# _parse_page_impl  (mocking parse_listing to avoid real HTTP)
# ---------------------------------------------------------------------------
class TestParsePageImpl:
    """Tests for search results page parsing."""

    def test_parses_all_listings_from_search_page(self, kijiji_scraper, search_page_soup):
        dummy_listing = MagicMock(spec=RentalsListing)

        with patch.object(kijiji_scraper, "parse_listing", return_value=dummy_listing):
            listings, failed = kijiji_scraper._parse_page_impl(search_page_soup, City.TORONTO)

        assert len(listings) == 3
        assert failed == 0

    def test_counts_failed_listings(self, kijiji_scraper, search_page_soup):
        # First call succeeds, second and third fail
        dummy_listing = MagicMock(spec=RentalsListing)
        side_effects = [dummy_listing, None, None]

        with patch.object(kijiji_scraper, "parse_listing", side_effect=side_effects):
            listings, failed = kijiji_scraper._parse_page_impl(search_page_soup, City.TORONTO)

        assert len(listings) == 1
        assert failed == 2

    def test_returns_empty_when_no_json_ld(self, kijiji_scraper, search_empty_soup):
        listings, failed = kijiji_scraper._parse_page_impl(search_empty_soup, City.TORONTO)
        assert listings == []
        assert failed == 0

    def test_returns_empty_on_unexpected_exception(self, kijiji_scraper, search_page_soup):
        with patch.object(
            kijiji_scraper,
            "_extract_json_ld",
            side_effect=RuntimeError("something broke"),
        ):
            listings, failed = kijiji_scraper._parse_page_impl(search_page_soup, City.TORONTO)

        assert listings == []
        assert failed == 0


# ---------------------------------------------------------------------------
# parse_listing (integration-level: mocking HTTP only)
# ---------------------------------------------------------------------------
class TestParseListing:
    """Tests for parse_listing (delegates to _parse_listing_detail)."""

    def test_returns_listing_for_valid_item(self, kijiji_scraper, listing_detail_html):
        item = {
            "item": {
                "url": "https://www.kijiji.ca/v-apartments-condos/toronto/beautiful-2br-apartment/123456789"
            }
        }
        mock_response = MagicMock()
        mock_response.text = listing_detail_html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            listing = kijiji_scraper.parse_listing(item, City.TORONTO)

        assert listing is not None
        assert listing.listing_id == "123456789"

    def test_returns_none_when_item_has_no_url(self, kijiji_scraper):
        item = {"item": {}}
        listing = kijiji_scraper.parse_listing(item, City.TORONTO)
        assert listing is None

    def test_returns_none_when_item_is_empty(self, kijiji_scraper):
        listing = kijiji_scraper.parse_listing({}, City.TORONTO)
        assert listing is None


# ---------------------------------------------------------------------------
# get_page (mocking HTTP)
# ---------------------------------------------------------------------------
class TestGetPage:
    """Tests for get_page HTTP fetching."""

    def test_returns_soup_object(self, kijiji_scraper):
        html = "<html><body><h1>Test</h1></body></html>"
        mock_response = MagicMock()
        mock_response.text = html
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            soup = kijiji_scraper.get_page(City.TORONTO, page=1)

        assert isinstance(soup, BeautifulSoup)
        assert soup.find("h1").text == "Test"

    def test_passes_correct_url_and_params(self, kijiji_scraper):
        mock_response = MagicMock()
        mock_response.text = "<html></html>"
        mock_response.raise_for_status = MagicMock()

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response) as mock_get:
            kijiji_scraper.get_page(City.TORONTO, page=3)

        expected_url = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/c37l1700273"
        mock_get.assert_called_once_with(expected_url, params={"page": 3}, timeout=10)

    def test_raises_on_http_error(self, kijiji_scraper):
        import requests

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")

        with patch.object(kijiji_scraper.session, "get", return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                kijiji_scraper.get_page(City.TORONTO)


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------
class TestContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_closes_session(self):
        with KijijiScraper(user_agent="TestAgent/1.0", download_delay=0) as scraper:
            assert scraper.session is not None

    def test_enter_returns_self(self):
        scraper = KijijiScraper(user_agent="TestAgent/1.0", download_delay=0)
        result = scraper.__enter__()
        assert result is scraper
        scraper.close()
