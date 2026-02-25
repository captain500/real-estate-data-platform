"""Web scraper for Kijiji.ca listings."""

import json
import logging
import random
from datetime import datetime
from time import sleep

from bs4 import BeautifulSoup

from real_estate_data_platform.models.enums import City
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.scrapers.base_scraper import BaseScraper
from real_estate_data_platform.utils.parsers import parse_float, parse_int

logger = logging.getLogger(__name__)

# Mapping of Kijiji attributes to RentalsListing fields
ATTRIBUTE_MAPPING = {
    "Bedrooms": "bedrooms",
    "Bathrooms": "bathrooms",
    "Size (sqft)": "size_sqft",
    "Unit Type": "unit_type",
    "Agreement Type": "agreement_type",
    "Furnished": "furnished",
    "For Rent By": "for_rent_by",
    "Elevator in Building": "elevator",
    "Gym": "gym",
    "Concierge": "concierge",
    "24 Hour Security": "security_24h",
    "Pool": "pool",
    "Balcony": "balcony",
    "Yard": "yard",
    "Storage Space": "storage_space",
    "Heat": "heat",
    "Water": "water",
    "Hydro": "hydro",
    "Internet": "internet",
    "Cable / TV": "cable_tv",
    "Laundry (In Unit)": "laundry_in_unit",
    "Laundry (In Building)": "laundry_in_building",
    "Parking Included": "parking_included",
    "Dishwasher": "dishwasher",
    "Fridge / Freezer": "fridge_freezer",
    "Pet Friendly": "pet_friendly",
    "Smoking Permitted": "smoking_permitted",
    "Wheelchair accessible": "wheelchair_accessible",
    "Barrier-free Entrances and Ramps": "barrier_free",
    "Accessible Washrooms in Suite": "accessible_washrooms",
    "Audio Prompts": "audio_prompts",
    "Visual Aids": "visual_aids",
    "Braille Labels": "braille_labels",
    "Bicycle Parking": "bicycle_parking",
    "Air Conditioning": "air_conditioning",
    "Move-In Date": "move_in_date",
}


class KijijiScraper(BaseScraper):
    """Scraper for Kijiji.ca rental listings."""

    @property
    def NAME_WEBSITE(self) -> str:
        return "kijiji"

    @property
    def BASE_URL(self) -> str:
        return "https://www.kijiji.ca/b-apartments-condos"

    @property
    def SUPPORTED_CITIES(self) -> dict[City, str]:
        return {
            City.TORONTO: "city-of-toronto/c37l1700273",
            City.VANCOUVER: "vancouver/c37l1700287",
            City.LONDON: "london/c37l1700214",
        }

    def get_page(self, city: City, page: int = 1) -> BeautifulSoup:
        """Fetch and parse a search results page from Kijiji.

        Args:
            city: City to search (City enum value)
            page: Page number (1-based)

        Returns:
            BeautifulSoup object of the page

        Raises:
            requests.HTTPError: If HTTP request fails
            ValueError: If city not supported
        """
        if city not in self.SUPPORTED_CITIES:
            supported = [c.value for c in self.SUPPORTED_CITIES.keys()]
            raise ValueError(f"City '{city.value}' not supported. Supported: {supported}")

        city_path = self.SUPPORTED_CITIES[city]
        url = f"{self.BASE_URL}/{city_path}"

        logger.info(f"Fetching {url} (page {page})")
        response = self.session.get(url, params={"page": page}, timeout=10)
        response.raise_for_status()

        return BeautifulSoup(response.text, "html.parser")

    def parse_listing(self, listing_elem: dict, city: str) -> RentalsListing | None:
        """Parse a single listing element from search results.

        Args:
            listing_elem: Dictionary from itemListElement (JSON-LD)
            city: City name (string value of City enum)

        Returns:
            RentalsListing object or None if parsing fails
        """
        try:
            url = listing_elem.get("item", {}).get("url")
            return self._parse_listing_detail(url, city) if url else None
        except Exception as e:
            logger.warning(f"Error parsing search listing: {e}")
            return None

    def _parse_page_impl(
        self, soup: BeautifulSoup, city: City, download_delay: float = 2.0
    ) -> list[RentalsListing]:
        """Internal implementation for parsing all listings from a search results page.

        Args:
            soup: BeautifulSoup object of the search page
            city: City being scraped (City enum value)
            download_delay: Delay in seconds between requests to listing pages

        Returns:
            List of parsed RentalsListing objects (filtering handled by parse_page)
        """
        listings = []

        try:
            data = self._extract_json_ld(soup)
            if not data:
                logger.warning(f"No JSON-LD data found for {city.value} search page")
                return listings

            for item in data.get("itemListElement", []):
                listing = self.parse_listing(item, city.value)
                if listing:
                    listings.append(listing)
                if download_delay > 0:
                    sleep(self.download_delay * random.uniform(0.5, 1.5))
                # TODO: Remove this break after testing
                if len(listings) >= 8:
                    break
        except Exception as e:
            logger.error(f"Error parsing search page for {city.value}: {e}")

        return listings

    def _parse_listing_detail(self, url: str, city: str) -> RentalsListing | None:
        """Parse a single listing detail page.

        Args:
            url: Full URL of the listing page
            city: City name (string value of City enum)

        Returns:
            RentalsListing object or None if parsing fails
        """
        try:
            logger.info(f"Fetching listing: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            script = soup.find("script", id="__NEXT_DATA__")

            if not script or not script.string:
                logger.warning(f"No __NEXT_DATA__ script found for {url}")
                return None

            data = json.loads(script.string)
            page_props = data.get("props", {}).get("pageProps", {})
            listing_id = page_props.get("listingId")

            if not listing_id:
                logger.warning(f"No listing ID found for {url}")
                return None

            apollo_state = page_props.get("__APOLLO_STATE__", {})
            listing_data = apollo_state.get(f"RealEstateListing:{listing_id}", {})

            if not listing_data:
                logger.warning(f"No listing data found for {listing_id}")
                return None

            # Extract and map attributes
            attributes = self._extract_attributes(listing_data)

            # Extract neighbourhood and scores
            neighbourhood, scores = self._extract_neighbourhood_info(listing_data, apollo_state)

            # Extract price
            price = listing_data.get("price", {}).get("amount", 0)
            if isinstance(price, int | float) and price > 100:
                price = price / 100

            activation_date = listing_data.get("activationDate") or page_props.get("activationDate")
            published_at = self._parse_activation_date(activation_date)

            # Build RentalsListing with mapped attributes
            listing = RentalsListing(
                listing_id=str(listing_id),
                url=url,
                website=self.NAME_WEBSITE,
                published_at=published_at,
                title=listing_data.get("title"),
                description=listing_data.get("description"),
                street=listing_data.get("location", {}).get("address"),
                city=city,
                neighbourhood=neighbourhood,
                rent=price,
                bedrooms=parse_int(attributes.get("bedrooms")),
                bathrooms=parse_int(attributes.get("bathrooms")),
                size_sqft=parse_float(attributes.get("size_sqft")),
                latitude=listing_data.get("location", {}).get("coordinates", {}).get("latitude"),
                longitude=listing_data.get("location", {}).get("coordinates", {}).get("longitude"),
                images=listing_data.get("imageUrls") or [],
                **{
                    k: attributes.get(k)
                    for k in ATTRIBUTE_MAPPING.values()
                    if k not in {"bedrooms", "bathrooms", "size_sqft"}
                },
                **scores,
            )
            return listing

        except Exception as e:
            logger.warning(f"Error parsing listing {url}: {e}")
            return None

    def _extract_json_ld(self, soup: BeautifulSoup) -> dict | None:
        """Extract JSON-LD data from page.

        Args:
            soup: BeautifulSoup object

        Returns:
            Parsed JSON data or None
        """
        script = soup.find("script", type="application/ld+json")
        if not script or not script.string:
            return None

        try:
            return json.loads(script.string)
        except json.JSONDecodeError:
            return None

    def _extract_attributes(self, listing_data: dict) -> dict:
        """Extract and normalize attributes from listing.

        Args:
            listing_data: Listing data dictionary

        Returns:
            Dictionary with normalized attributes
        """
        attributes_raw = listing_data.get("attributes", {}).get("all", [])
        attributes_raw_dict = {
            attr["name"]: attr.get("values", [None])[0] for attr in attributes_raw
        }

        # Map to RentalsListing field names
        return {
            ATTRIBUTE_MAPPING.get(k, k): v
            for k, v in attributes_raw_dict.items()
            if k in ATTRIBUTE_MAPPING
        }

    def _extract_neighbourhood_info(self, listing_data: dict, apollo_state: dict) -> tuple:
        """Extract neighbourhood name and scores.

        Args:
            listing_data: Listing data dictionary
            apollo_state: Apollo state from JSON

        Returns:
            Tuple of (neighbourhood_name, scores_dict)
        """
        neighbourhood = "N/A"
        scores = {"walk_score": None, "transit_score": None, "bike_score": None}

        neighbourhood_ref = (
            listing_data.get("location", {}).get("neighbourhoodInfo", {}).get("__ref")
        )

        if neighbourhood_ref and neighbourhood_ref in apollo_state:
            neighbourhood_data = apollo_state[neighbourhood_ref]
            neighbourhood = neighbourhood_data.get("name", "N/A")

            transport_scores = neighbourhood_data.get("scores", {}).get("transportation", {})
            scores = {
                "walk_score": transport_scores.get("walk", {}).get("score"),
                "transit_score": transport_scores.get("transit", {}).get("score"),
                "bike_score": transport_scores.get("cycle", {}).get("score"),
            }

        return neighbourhood, scores

    def _parse_activation_date(self, value) -> datetime:
        """Parse activation date in ISO format like '2026-02-12T08:03:15.000Z'."""
        try:
            normalized = str(value).replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
